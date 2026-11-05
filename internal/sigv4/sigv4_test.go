package sigv4

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	testAccess = "AKIDEXAMPLE"
	testSecret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	testRegion = "us-east-1"
)

// signWithSDK uses the official AWS signer as an independent reference.
func signWithSDK(t testing.TB, method, target string, body []byte, rangeHeader string) *http.Request {
	t.Helper()
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	req, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	signer := v4.NewSigner()
	err = signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID:     testAccess,
		SecretAccessKey: testSecret,
	}, req, payloadHash, "s3", testRegion, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestVerifyAcceptsSDKSignature(t *testing.T) {
	cases := []struct {
		method, target, rng string
		body                []byte
	}{
		{http.MethodGet, "https://s3.example.com/archive/video.mp4", "bytes=0-1023", nil},
		{http.MethodGet, "https://s3.example.com/archive/video.mp4", "", nil},
		{http.MethodPut, "https://s3.example.com/archive/hello.txt", "", []byte("hello world")},
		{http.MethodDelete, "https://s3.example.com/archive/hello.txt", "", nil},
		{http.MethodGet, "https://s3.example.com/archive?list-type=2&prefix=a%2Fb", "", nil},
	}
	for _, tc := range cases {
		req := signWithSDK(t, tc.method, tc.target, tc.body, tc.rng)
		if err := Verify(req, testAccess, testSecret, testRegion); err != nil {
			t.Errorf("%s %s: official signature rejected: %v", tc.method, tc.target, err)
		}
	}
}

func TestVerifyRejectsTampering(t *testing.T) {
	req := signWithSDK(t, http.MethodGet, "https://s3.example.com/archive/video.mp4", nil, "bytes=0-1023")
	req.Header.Set("Range", "bytes=0-2047")
	if err := Verify(req, testAccess, testSecret, testRegion); err == nil {
		t.Fatal("changed range must invalidate the signature")
	}

	wrongKey := signWithSDK(t, http.MethodGet, "https://s3.example.com/x", nil, "")
	if err := Verify(wrongKey, "OUTRA", testSecret, testRegion); err == nil {
		t.Fatal("different access key must fail")
	}
}

func TestParseRange(t *testing.T) {
	cases := []struct {
		header         string
		total          int64
		offset, length int64
		ok             bool
	}{
		{"bytes=0-9", 100, 0, 10, true},
		{"bytes=90-", 100, 90, 10, true},
		{"bytes=-10", 100, 90, 10, true},
		{"bytes=90-200", 100, 90, 10, true},
		{"bytes=0-9", 0, 0, 10, true},
	}
	for _, tc := range cases {
		offset, length, ok := ParseRange(tc.header, tc.total)
		if ok != tc.ok || offset != tc.offset || length != tc.length {
			t.Errorf("%s total=%d => %d,%d,%v", tc.header, tc.total, offset, length, ok)
		}
	}
	for _, bad := range []string{"", "items=0-1", "bytes=5-2", "bytes=abc"} {
		if _, _, ok := ParseRange(bad, 100); ok {
			t.Errorf("%q must be invalid", bad)
		}
	}
}

func BenchmarkVerify(b *testing.B) {
	req := signWithSDK(b, http.MethodGet, "https://s3.example.com/archive/video.mp4", nil, "bytes=0-1023")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Verify(req, testAccess, testSecret, testRegion); err != nil {
			b.Fatal(err)
		}
	}
}
