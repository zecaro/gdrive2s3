package drive

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestPropertyValueStaysWithinDriveLimit(t *testing.T) {
	casos := []struct {
		key   string
		cabe  bool
	}{
		{"media/gen_4290d7f1-db17-4a4f-8c31-92f374a3a799", true},
		{"chat-uploads/120363412807600499@lid/1751234567-a1b2c3d4-anexo.pdf", true},
		{"users/9f2c1a7b/exec/" + strings.Repeat("y", 120), false},
		{strings.Repeat("a", drivePropertyLimit-len(keyProperty)), true},
		{strings.Repeat("a", drivePropertyLimit-len(keyProperty)+1), false},
		{strings.Repeat("b", 400), false},
	}
	for _, c := range casos {
		value, ok := propertyValue(c.key)
		if ok != c.cabe {
			t.Fatalf("chave de %d bytes: cabe=%v, esperado %v", len(c.key), ok, c.cabe)
		}
		if ok && value != c.key {
			t.Fatalf("chave que cabe deveria ser preservada: %q virou %q", c.key, value)
		}
		if ok && len(keyProperty)+len(value) > drivePropertyLimit {
			t.Fatalf("propriedade de %d bytes estoura o limite %d", len(keyProperty)+len(value), drivePropertyLimit)
		}
	}
}
