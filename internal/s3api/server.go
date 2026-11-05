// Package s3api exposes the subset of the S3 API used by the backup and the BFF:
// list, put, get with Range, head and delete, with SigV4 authentication.
package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zecaro/gdrive2s3/internal/sigv4"
	"github.com/zecaro/gdrive2s3/internal/store"
)

type Config struct {
	AccessKey string
	SecretKey string
	Region    string
	Buckets   map[string]bool
	Logger    *slog.Logger
}

type Server struct {
	store storeAPI
	cfg   Config
	log   *slog.Logger
	nowFn func() time.Time
}

// storeAPI is the contract used by the handlers, so tests run without Drive.
type storeAPI interface {
	Put(ctx context.Context, bucket, key string, body io.Reader, size int64, contentType string) (string, error)
	Get(ctx context.Context, bucket, key string, offset, length int64) (io.ReadCloser, int64, int, string, string, error)
	Stat(ctx context.Context, bucket, key string) (int64, string, string, error)
	Delete(ctx context.Context, bucket, key string) error
	List(ctx context.Context, bucket, prefix, startAfter string, max int) ([]store.Object, bool, error)
}

func New(st storeAPI, cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: st, cfg: cfg, log: logger, nowFn: time.Now}
}

func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// Health without a signature: systemd and the tunnel probe it.
	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	if err := sigv4.Verify(r, s.cfg.AccessKey, s.cfg.SecretKey, s.cfg.Region); err != nil {
		code := "SignatureDoesNotMatch"
		if errors.Is(err, sigv4.ErrMissingAuth) {
			code = "AccessDenied"
		}
		s.log.Warn("signature rejected", "err", err, "path", r.URL.Path)
		writeError(w, http.StatusForbidden, code, err.Error())
		return
	}

	bucket, key := splitPath(r.URL.Path)
	if bucket == "" {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "missing bucket")
		return
	}
	if !s.cfg.Buckets[bucket] {
		writeError(w, http.StatusNotFound, "NoSuchBucket", "bucket not configured")
		return
	}

	ctx := r.Context()
	switch {
	case key == "" && r.Method == http.MethodGet:
		s.listOrMeta(w, r, bucket)
	case key == "" && r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case key == "":
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed on bucket")
	case r.Method == http.MethodPut:
		s.put(ctx, w, r, bucket, key)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		s.get(ctx, w, r, bucket, key)
	case r.Method == http.MethodDelete:
		s.delete(ctx, w, bucket, key)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

// splitPath accepts path-style: /bucket or /bucket/composite/key.
func splitPath(path string) (string, string) {
	path = strings.TrimPrefix(path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	return bucket, key
}

func (s *Server) listOrMeta(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	if _, ok := query["location"]; ok {
		type location struct {
			XMLName xml.Name `xml:"LocationConstraint"`
			Value   string   `xml:",chardata"`
		}
		writeXML(w, http.StatusOK, location{Value: s.cfg.Region})
		return
	}
	if _, ok := query["uploads"]; ok {
		writeXML(w, http.StatusOK, struct {
			XMLName xml.Name `xml:"ListMultipartUploadsResult"`
			Bucket  string   `xml:"Bucket"`
		}{Bucket: bucket})
		return
	}

	max := 1000
	if raw := query.Get("max-keys"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 1000 {
			max = parsed
		}
	}
	startAfter := query.Get("start-after")
	if token := query.Get("continuation-token"); token != "" {
		startAfter = token
	}
	objects, truncated, err := s.store.List(r.Context(), bucket, query.Get("prefix"), startAfter, max)
	if err != nil {
		s.fail(w, err)
		return
	}

	type content struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	type result struct {
		XMLName               xml.Name  `xml:"ListBucketResult"`
		Xmlns                 string    `xml:"xmlns,attr"`
		Name                  string    `xml:"Name"`
		Prefix                string    `xml:"Prefix"`
		KeyCount              int       `xml:"KeyCount"`
		MaxKeys               int       `xml:"MaxKeys"`
		IsTruncated           bool      `xml:"IsTruncated"`
		NextContinuationToken string    `xml:"NextContinuationToken,omitempty"`
		Contents              []content `xml:"Contents"`
	}
	out := result{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		Prefix:      query.Get("prefix"),
		KeyCount:    len(objects),
		MaxKeys:     max,
		IsTruncated: truncated,
	}
	for _, object := range objects {
		out.Contents = append(out.Contents, content{
			Key:          object.Key,
			LastModified: object.Modified.UTC().Format(time.RFC3339),
			ETag:         `"` + object.ETag + `"`,
			Size:         object.Size,
			StorageClass: "STANDARD",
		})
	}
	if truncated && len(objects) > 0 {
		out.NextContinuationToken = objects[len(objects)-1].Key
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) put(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	if _, ok := r.URL.Query()["uploads"]; ok {
		writeError(w, http.StatusNotImplemented, "NotImplemented", "multipart not supported; use a single PUT")
		return
	}
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		writeError(w, http.StatusNotImplemented, "NotImplemented", "multipart not supported; use a single PUT")
		return
	}
	if r.ContentLength < 0 {
		writeError(w, http.StatusLengthRequired, "MissingContentLength", "envie Content-Length")
		return
	}
	etag, err := s.store.Put(ctx, bucket, key, r.Body, r.ContentLength, r.Header.Get("Content-Type"))
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) get(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	offset, length := int64(0), int64(0)
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		// Total size is only known later; validate the syntax with total 0 here.
		parsedOffset, parsedLength, ok := sigv4.ParseRange(rangeHeader, 0)
		if !ok {
			w.Header().Set("Content-Range", "bytes */0")
			writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "invalid range")
			return
		}
		offset, length = parsedOffset, parsedLength
	}

	body, total, status, etag, contentType, err := s.store.Get(ctx, bucket, key, offset, length)
	if err != nil {
		s.fail(w, err)
		return
	}
	defer body.Close()

	if status == http.StatusRequestedRangeNotSatisfiable {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "range not satisfiable")
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", contentType)
	if etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	if rangeHeader != "" && status == http.StatusPartialContent && length > 0 {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, total))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(total, 10))
		w.WriteHeader(http.StatusOK)
	}
	if r.Method == http.MethodHead {
		return
	}
	if _, err := io.Copy(w, body); err != nil {
		s.log.Warn("copy interrupted", "key", key, "err", err)
	}
}

func (s *Server) delete(ctx context.Context, w http.ResponseWriter, bucket, key string) {
	if err := s.store.Delete(ctx, bucket, key); err != nil && !errors.Is(err, store.ErrNotFound) {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchKey", "no such key")
		return
	}
	s.log.Error("handler failed", "err", err)
	writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
}

type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeXML(w, status, s3Error{Code: code, Message: message})
}

func writeXML(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(payload)
}
