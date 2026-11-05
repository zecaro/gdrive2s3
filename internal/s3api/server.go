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
	Put(ctx context.Context, bucket, key string, body io.Reader, size int64, contentType string, metadata map[string]string) (string, error)
	Get(ctx context.Context, bucket, key string, offset, length int64) (io.ReadCloser, int64, int, string, string, map[string]string, error)
	Stat(ctx context.Context, bucket, key string) (int64, string, string, map[string]string, error)
	Delete(ctx context.Context, bucket, key string) error
	List(ctx context.Context, bucket, prefix, startAfter string, max int) ([]store.Object, bool, error)
	CreateMultipart(bucket, key, contentType string, metadata map[string]string) (string, error)
	PutPart(uploadID string, partNumber int, body io.Reader) (string, error)
	CompleteMultipart(ctx context.Context, uploadID string, parts []store.Part) (string, error)
	AbortMultipart(uploadID string) error
	UploadExists(uploadID string) (store.MultipartUpload, error)
}

func New(st storeAPI, cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: st, cfg: cfg, log: logger, nowFn: time.Now}
}

func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

// userMetadata lê os headers x-amz-meta-* e devolve sem o prefixo.
func userMetadata(r *http.Request) map[string]string {
	out := map[string]string{}
	for name, values := range r.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			out[lower[len("x-amz-meta-"):]] = values[0]
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func writeUserMetadata(h http.Header, metadata map[string]string) {
	for key, value := range metadata {
		h.Set("x-amz-meta-"+key, value)
	}
}

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
	case r.Method == http.MethodPost && key != "":
		s.post(ctx, w, r, bucket, key)
	case r.Method == http.MethodPut:
		s.put(ctx, w, r, bucket, key)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		s.get(ctx, w, r, bucket, key)
	case r.Method == http.MethodDelete:
		if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
			if err := s.store.AbortMultipart(uploadID); err != nil {
				s.fail(w, err)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
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
	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		s.putPart(w, r, uploadID)
		return
	}
	if r.ContentLength < 0 {
		writeError(w, http.StatusLengthRequired, "MissingContentLength", "envie Content-Length")
		return
	}
	etag, err := s.store.Put(ctx, bucket, key, r.Body, r.ContentLength, r.Header.Get("Content-Type"), userMetadata(r))
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) get(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	if r.Method == http.MethodHead {
		size, etag, contentType, metadata, err := s.store.Stat(ctx, bucket, key)
		if err != nil {
			s.fail(w, err)
			return
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		if etag != "" {
			w.Header().Set("ETag", `"`+etag+`"`)
		}
		writeUserMetadata(w.Header(), metadata)
		w.WriteHeader(http.StatusOK)
		return
	}
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

	body, total, status, etag, contentType, metadata, err := s.store.Get(ctx, bucket, key, offset, length)
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
	writeUserMetadata(w.Header(), metadata)
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

// post trata o início (POST ?uploads) e a conclusão (POST ?uploadId) do multipart.
func (s *Server) post(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	query := r.URL.Query()
	if _, ok := query["uploads"]; ok {
		uploadID, err := s.store.CreateMultipart(bucket, key, r.Header.Get("Content-Type"), userMetadata(r))
		if err != nil {
			s.fail(w, err)
			return
		}
		type initiate struct {
			XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
			Bucket   string   `xml:"Bucket"`
			Key      string   `xml:"Key"`
			UploadID string   `xml:"UploadId"`
		}
		writeXML(w, http.StatusOK, initiate{Bucket: bucket, Key: key, UploadID: uploadID})
		return
	}

	uploadID := query.Get("uploadId")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "missing uploadId")
		return
	}
	var payload struct {
		Parts []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "MalformedXML", "invalid complete body")
		return
	}
	parts := make([]store.Part, 0, len(payload.Parts))
	for _, part := range payload.Parts {
		parts = append(parts, store.Part{Number: part.PartNumber, ETag: part.ETag})
	}
	etag, err := s.store.CompleteMultipart(ctx, uploadID, parts)
	if err != nil {
		s.fail(w, err)
		return
	}
	type complete struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	writeXML(w, http.StatusOK, complete{Bucket: bucket, Key: key, ETag: `"` + etag + `"`})
}

func (s *Server) putPart(w http.ResponseWriter, r *http.Request, uploadID string) {
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || partNumber < 1 || partNumber > 10000 {
		writeError(w, http.StatusBadRequest, "InvalidPart", "invalid partNumber")
		return
	}
	etag, err := s.store.PutPart(uploadID, partNumber, r.Body)
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}
