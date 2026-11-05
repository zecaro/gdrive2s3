package s3api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/zecaro/gdrive2s3/internal/store"
)

const (
	testAccess = "AKIDEXAMPLE"
	testSecret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	testRegion = "us-east-1"
)

type fakeStore struct {
	objects map[string][]byte
	uploads map[string]store.MultipartUpload
	parts   map[string]map[int][]byte
	nextID  int
}

func newFake() *fakeStore {
	return &fakeStore{objects: map[string][]byte{}, uploads: map[string]store.MultipartUpload{}, parts: map[string]map[int][]byte{}}
}

func (f *fakeStore) CreateMultipart(bucket, key, contentType string) (string, error) {
	f.nextID++
	id := "mpu-" + string(rune('0'+f.nextID))
	f.uploads[id] = store.MultipartUpload{ID: id, Bucket: bucket, Key: key, ContentType: contentType}
	f.parts[id] = map[int][]byte{}
	return id, nil
}

func (f *fakeStore) PutPart(uploadID string, partNumber int, body io.Reader) (string, error) {
	if _, ok := f.parts[uploadID]; !ok {
		return "", store.ErrNotFound
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	f.parts[uploadID][partNumber] = data
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (f *fakeStore) CompleteMultipart(_ context.Context, uploadID string, parts []store.Part) (string, error) {
	upload, ok := f.uploads[uploadID]
	if !ok {
		return "", store.ErrNotFound
	}
	var buf bytes.Buffer
	for _, part := range parts {
		data, ok := f.parts[uploadID][part.Number]
		if !ok {
			return "", store.ErrNotFound
		}
		buf.Write(data)
	}
	return f.Put(context.Background(), upload.Bucket, upload.Key, bytes.NewReader(buf.Bytes()), int64(buf.Len()), upload.ContentType)
}

func (f *fakeStore) AbortMultipart(uploadID string) error {
	delete(f.uploads, uploadID)
	delete(f.parts, uploadID)
	return nil
}

func (f *fakeStore) UploadExists(uploadID string) (store.MultipartUpload, error) {
	upload, ok := f.uploads[uploadID]
	if !ok {
		return upload, store.ErrNotFound
	}
	return upload, nil
}

func (f *fakeStore) Put(_ context.Context, bucket, key string, body io.Reader, size int64, _ string) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	f.objects[bucket+"/"+key] = data
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (f *fakeStore) Get(_ context.Context, bucket, key string, offset, length int64) (io.ReadCloser, int64, int, string, string, error) {
	data, ok := f.objects[bucket+"/"+key]
	if !ok {
		return nil, 0, 0, "", "", store.ErrNotFound
	}
	total := int64(len(data))
	if length <= 0 {
		return io.NopCloser(bytes.NewReader(data)), total, http.StatusOK, "etag", "video/mp4", nil
	}
	end := offset + length
	if end > total {
		end = total
	}
	return io.NopCloser(bytes.NewReader(data[offset:end])), total, http.StatusPartialContent, "etag", "video/mp4", nil
}

func (f *fakeStore) Stat(_ context.Context, bucket, key string) (int64, string, string, error) {
	data, ok := f.objects[bucket+"/"+key]
	if !ok {
		return 0, "", "", store.ErrNotFound
	}
	return int64(len(data)), "etag", "video/mp4", nil
}

func (f *fakeStore) Delete(_ context.Context, bucket, key string) error {
	delete(f.objects, bucket+"/"+key)
	return nil
}

func (f *fakeStore) List(_ context.Context, bucket, prefix, startAfter string, max int) ([]store.Object, bool, error) {
	var out []store.Object
	for k, v := range f.objects {
		b, key, _ := strings.Cut(k, "/")
		if b != bucket || !strings.HasPrefix(key, prefix) || key <= startAfter {
			continue
		}
		out = append(out, store.Object{Key: key, Size: int64(len(v)), ETag: "etag", Modified: time.Unix(0, 0)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, false, nil
}

func newServer() *Server {
	return New(newFake(), Config{AccessKey: testAccess, SecretKey: testSecret, Region: testRegion,
		Buckets: map[string]bool{"archive": true}})
}

func signed(t testing.TB, method, target string, body []byte, rangeHeader string) *http.Request {
	t.Helper()
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	err := v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAccess, SecretAccessKey: testSecret},
		req, payloadHash, "s3", testRegion, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestGetRange(t *testing.T) {
	srv := newServer()
	put := signed(t, http.MethodPut, "https://s3.test/archive/video.mp4", []byte("0123456789"), "")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, put)
	if rec.Code != http.StatusOK {
		t.Fatalf("put=%d", rec.Code)
	}

	get := signed(t, http.MethodGet, "https://s3.test/archive/video.mp4", nil, "bytes=2-4")
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, get)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("get=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "234" {
		t.Fatalf("body=%q", got)
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes 2-4/10" {
		t.Fatalf("content-range=%q", got)
	}
}

func TestUnsignedRejected(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://s3.test/archive/video.mp4", nil)
	rec := httptest.NewRecorder()
	newServer().Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestListXML(t *testing.T) {
	srv := newServer()
	for _, key := range []string{"a/1.mov", "a/2.mov", "b/3.mov"} {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, signed(t, http.MethodPut, "https://s3.test/archive/"+key, []byte("x"), ""))
	}
	req := signed(t, http.MethodGet, "https://s3.test/archive?list-type=2&prefix=a%2F", nil, "")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list=%d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		KeyCount int `xml:"KeyCount"`
		Contents []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("xml: %v", err)
	}
	if out.KeyCount != 2 || len(out.Contents) != 2 || out.Contents[0].Key != "a/1.mov" {
		t.Fatalf("list=%+v", out)
	}
}

func TestHeadAndDelete(t *testing.T) {
	srv := newServer()
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, signed(t, http.MethodPut, "https://s3.test/archive/x.txt", []byte("abc"), ""))
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, signed(t, http.MethodHead, "https://s3.test/archive/x.txt", nil, ""))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Length") != "3" {
		t.Fatalf("head=%d len=%s", rec.Code, rec.Header().Get("Content-Length"))
	}
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, signed(t, http.MethodDelete, "https://s3.test/archive/x.txt", nil, ""))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete=%d", rec.Code)
	}
}

func BenchmarkGetRange(b *testing.B) {
	srv := newServer()
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, signed(b, http.MethodPut, "https://s3.test/archive/v.mp4", bytes.Repeat([]byte("a"), 1<<20), ""))
	req := signed(b, http.MethodGet, "https://s3.test/archive/v.mp4", nil, "bytes=0-1023")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req.Clone(context.Background()))
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestMultipartFlow(t *testing.T) {
	srv := newServer()
	handler := srv.Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, signed(t, http.MethodPost, "https://s3.test/archive/big.bin?uploads", nil, ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("initiate=%d %s", rec.Code, rec.Body.String())
	}
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &init); err != nil || init.UploadID == "" {
		t.Fatalf("uploadId: %v %q", err, rec.Body.String())
	}

	part1 := []byte("hello ")
	part2 := []byte("world")
	etags := make([]string, 0, 2)
	for i, part := range [][]byte{part1, part2} {
		rec = httptest.NewRecorder()
		target := "https://s3.test/archive/big.bin?partNumber=" + itoa(i+1) + "&uploadId=" + init.UploadID
		handler.ServeHTTP(rec, signed(t, http.MethodPut, target, part, ""))
		if rec.Code != http.StatusOK {
			t.Fatalf("part %d = %d %s", i+1, rec.Code, rec.Body.String())
		}
		etags = append(etags, rec.Header().Get("ETag"))
	}

	complete := "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + etags[0] +
		"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>" + etags[1] + "</ETag></Part></CompleteMultipartUpload>"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, signed(t, http.MethodPost, "https://s3.test/archive/big.bin?uploadId="+init.UploadID, []byte(complete), ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("complete=%d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, signed(t, http.MethodGet, "https://s3.test/archive/big.bin", nil, ""))
	if got := rec.Body.String(); got != "hello world" {
		t.Fatalf("corpo=%q", got)
	}
}
