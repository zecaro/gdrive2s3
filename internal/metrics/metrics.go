// Package metrics keeps the counters exposed on the private metrics listener.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

type Registry struct {
	mu       sync.Mutex
	requests map[string]map[int]int64
	bytesOut int64
}

func New() *Registry { return &Registry{requests: map[string]map[int]int64{}} }

func (r *Registry) Observe(method string, status int, bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.requests[method] == nil {
		r.requests[method] = map[int]int64{}
	}
	r.requests[method][status]++
	r.bytesOut += bytes
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		defer r.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintln(w, "# TYPE gdrive2s3_requests_total counter")
		methods := make([]string, 0, len(r.requests))
		for method := range r.requests {
			methods = append(methods, method)
		}
		sort.Strings(methods)
		for _, method := range methods {
			codes := make([]int, 0, len(r.requests[method]))
			for code := range r.requests[method] {
				codes = append(codes, code)
			}
			sort.Ints(codes)
			for _, code := range codes {
				fmt.Fprintf(w, "gdrive2s3_requests_total{method=%q,code=%q} %d\n", method, fmt.Sprint(code), r.requests[method][code])
			}
		}
		fmt.Fprintln(w, "# TYPE gdrive2s3_bytes_total counter")
		fmt.Fprintf(w, "gdrive2s3_bytes_total %d\n", r.bytesOut)
	})
}

func Middleware(reg *Registry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		reg.Observe(r.Method, rec.status, rec.bytes)
	})
}

type recorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *recorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }

func (r *recorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}
