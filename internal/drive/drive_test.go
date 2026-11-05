package drive

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/oauth2"
)

func TestOpenRangeForwardsRangeAndParses206(t *testing.T) {
	var seenRange string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenRange = r.Header.Get("Range")
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Range", "bytes 10-14/100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("01234"))
	}))
	defer srv.Close()

	client := &Client{
		hc:      srv.Client(),
		ts:      oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"}),
		apiBase: srv.URL,
	}
	body, total, status, err := client.OpenRange(context.Background(), "file-1", 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, _ := io.ReadAll(body)
	if seenRange != "bytes=10-14" {
		t.Fatalf("range enviado=%q", seenRange)
	}
	if status != http.StatusPartialContent || total != 100 || string(data) != "01234" {
		t.Fatalf("status=%d total=%d body=%q", status, total, data)
	}
}
