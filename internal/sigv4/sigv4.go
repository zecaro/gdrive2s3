// Package sigv4 verifies AWS Signature Version 4 requests (header auth) for the
// S3 API. It does not sign: the gateway only needs to check what the client sent.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	ErrMissingAuth = errors.New("missing signature")
	ErrMalformed   = errors.New("malformed signature")
	ErrSignature   = errors.New("signature mismatch")
	ErrExpired     = errors.New("signature expired")
)

const (
	algorithm  = "AWS4-HMAC-SHA256"
	maxSkew    = 15 * time.Minute
	iso8601    = "20060102T150405Z"
	shortDate  = "20060102"
	unsignedPl = "UNSIGNED-PAYLOAD"
)

// Verify checks the signature against the current clock.
func Verify(r *http.Request, accessKey, secretKey, region string) error {
	return VerifyAt(r, accessKey, secretKey, region, time.Now())
}

// VerifyAt lets tests inject the clock when using official AWS vectors.
func VerifyAt(r *http.Request, accessKey, secretKey, region string, now time.Time) error {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ErrMissingAuth
	}
	if !strings.HasPrefix(auth, algorithm+" ") {
		return ErrMalformed
	}
	fields := strings.Split(strings.TrimPrefix(auth, algorithm+" "), ",")
	credential, signedHeaders, signature := "", "", ""
	for _, field := range fields {
		field = strings.TrimSpace(field)
		switch {
		case strings.HasPrefix(field, "Credential="):
			credential = strings.TrimPrefix(field, "Credential=")
		case strings.HasPrefix(field, "SignedHeaders="):
			signedHeaders = strings.TrimPrefix(field, "SignedHeaders=")
		case strings.HasPrefix(field, "Signature="):
			signature = strings.TrimPrefix(field, "Signature=")
		}
	}
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || credential == "" || signedHeaders == "" || signature == "" {
		return ErrMalformed
	}
	if subtle.ConstantTimeCompare([]byte(parts[0]), []byte(accessKey)) != 1 {
		return ErrSignature
	}
	dateShort, scopeRegion, service := parts[1], parts[2], parts[3]
	if service != "s3" {
		return ErrMalformed
	}

	amzDate := r.Header.Get("X-Amz-Date")
	ts, err := time.Parse(iso8601, amzDate)
	if err != nil {
		return ErrMalformed
	}
	if delta := now.Sub(ts); delta > maxSkew || delta < -maxSkew {
		return ErrExpired
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = unsignedPl
	}

	canonical, err := canonicalRequest(r, signedHeaders, payloadHash)
	if err != nil {
		return err
	}
	scope := strings.Join([]string{dateShort, scopeRegion, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		scope,
		hexSHA256([]byte(canonical)),
	}, "\n")

	key := signingKey(secretKey, dateShort, scopeRegion, service)
	expected := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))
	if subtle.ConstantTimeCompare([]byte(expected), []byte(strings.ToLower(signature))) != 1 {
		return ErrSignature
	}
	return nil
}

func canonicalRequest(r *http.Request, signedHeaders, payloadHash string) (string, error) {
	headers := strings.Split(signedHeaders, ";")
	sort.Strings(headers)

	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(canonicalURI(r.URL))
	b.WriteByte('\n')
	b.WriteString(canonicalQuery(r.URL))
	b.WriteByte('\n')
	for _, h := range headers {
		value := ""
		switch h {
		case "host":
			value = r.Host
		case "content-length":
			// Go keeps Content-Length out of Header: it lives in r.ContentLength.
			if r.ContentLength >= 0 {
				value = strconv.FormatInt(r.ContentLength, 10)
			}
		default:
			value = strings.Join(r.Header.Values(h), ",")
		}
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(strings.Join(strings.Fields(value), " "))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(signedHeaders)
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String(), nil
}

// canonicalURI returns the already escaped path, encoding each segment once and
// preserving slashes, as S3 requires.
func canonicalURI(u *url.URL) string {
	if u.EscapedPath() == "" {
		return "/"
	}
	segments := strings.Split(u.EscapedPath(), "/")
	for i, segment := range segments {
		if unescaped, err := url.PathUnescape(segment); err == nil {
			segments[i] = awsEscape(unescaped)
		}
	}
	return strings.Join(segments, "/")
}

func canonicalQuery(u *url.URL) string {
	values := u.Query()
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		items := append([]string(nil), values[key]...)
		sort.Strings(items)
		for _, item := range items {
			parts = append(parts, awsEscape(key)+"="+awsEscape(item))
		}
	}
	return strings.Join(parts, "&")
}

func awsEscape(s string) string {
	escaped := url.QueryEscape(s)
	escaped = strings.ReplaceAll(escaped, "+", "%20")
	escaped = strings.ReplaceAll(escaped, "%7E", "~")
	return escaped
}

func signingKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ParseRange reads "bytes=start-end" and returns offset and length. An unknown
// size is signalled with -1 as length.
func ParseRange(header string, total int64) (offset, length int64, ok bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimSpace(strings.TrimPrefix(header, "bytes="))
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	startText, endText, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	if startText == "" {
		// suffix: last N bytes
		n, err := strconv.ParseInt(endText, 10, 64)
		if err != nil || n <= 0 || total <= 0 {
			return 0, 0, false
		}
		if n > total {
			n = total
		}
		return total - n, n, true
	}
	start, err := strconv.ParseInt(startText, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if total > 0 && start >= total {
		return 0, 0, false
	}
	if endText == "" {
		if total > 0 {
			return start, total - start, true
		}
		return start, -1, true
	}
	end, err := strconv.ParseInt(endText, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	if total > 0 && end >= total {
		end = total - 1
	}
	return start, end - start + 1, true
}

func formatBadRange(total int64) string {
	return fmt.Sprintf("bytes */%d", total)
}
