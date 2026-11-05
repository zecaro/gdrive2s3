package store

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zecaro/gdrive2s3/internal/errs"
)

type memBlobs struct {
	folders map[string]string
	files   map[string][]byte
}

func newMem() *memBlobs {
	return &memBlobs{folders: map[string]string{}, files: map[string][]byte{}}
}

func (m *memBlobs) EnsureFolder(_ context.Context, name string) (string, error) {
	if id, ok := m.folders[name]; ok {
		return id, nil
	}
	id := "folder-" + name
	m.folders[name] = id
	return id, nil
}

func (m *memBlobs) Put(_ context.Context, folderID, name string, body io.Reader, _ int64, _, _ string) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	key := folderID + "/" + name
	m.files[key] = data
	return key, nil
}

func (m *memBlobs) FindByName(_ context.Context, folderID, name string) (string, int64, error) {
	data, ok := m.files[folderID+"/"+name]
	if !ok {
		return "", 0, errs.ErrNotFound
	}
	return folderID + "/" + name, int64(len(data)), nil
}

func (m *memBlobs) OpenRange(_ context.Context, fileID string, offset, length int64) (io.ReadCloser, int64, int, error) {
	data, ok := m.files[fileID]
	if !ok {
		return nil, 0, 0, errs.ErrNotFound
	}
	total := int64(len(data))
	if length <= 0 {
		return io.NopCloser(bytes.NewReader(data)), total, 200, nil
	}
	end := offset + length
	if end > total {
		end = total
	}
	return io.NopCloser(bytes.NewReader(data[offset:end])), total, 206, nil
}

func (m *memBlobs) Delete(_ context.Context, fileID string) error {
	delete(m.files, fileID)
	return nil
}

func newStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(Config{Path: filepath.Join(t.TempDir(), "index.db"), Root: "root", Blobs: newMem()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestRoundTrip(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	etag, err := st.Put(ctx, "b", "a/one.txt", strings.NewReader("hello"), 5, "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if etag != "5d41402abc4b2a76b9719d911017c592" {
		t.Fatalf("etag=%s", etag)
	}
	body, total, status, gotETag, contentType, err := st.Get(ctx, "b", "a/one.txt", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, _ := io.ReadAll(body)
	if string(data) != "ell" || total != 5 || status != 206 || gotETag != etag || contentType != "text/plain" {
		t.Fatalf("get=%q total=%d status=%d etag=%s ct=%s", data, total, status, gotETag, contentType)
	}
	if err := st.Delete(ctx, "b", "a/one.txt"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, err := st.Get(ctx, "b", "a/one.txt", 0, 0); err == nil {
		t.Fatal("deleted object must disappear")
	}
}

func TestListPagination(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	for _, key := range []string{"p/1", "p/2", "p/3", "q/1"} {
		if _, err := st.Put(ctx, "b", key, strings.NewReader("x"), 1, ""); err != nil {
			t.Fatal(err)
		}
	}
	first, truncated, err := st.List(ctx, "b", "p/", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || !truncated || first[0].Key != "p/1" {
		t.Fatalf("primeira=%v truncated=%v", first, truncated)
	}
	second, truncated, err := st.List(ctx, "b", "p/", first[len(first)-1].Key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || truncated || second[0].Key != "p/3" {
		t.Fatalf("segunda=%v truncated=%v", second, truncated)
	}
}

func BenchmarkList1000(b *testing.B) {
	st, err := Open(Config{Path: filepath.Join(b.TempDir(), "bench.db"), Root: "root", Blobs: newMem()})
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		key := "media/" + strings.Repeat("a", 8) + string(rune('a'+i%26)) + "-" + itoa(i)
		if _, err := st.Put(ctx, "b", key, strings.NewReader("x"), 1, ""); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := st.List(ctx, "b", "media/", "", 1000); err != nil {
			b.Fatal(err)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func TestGetHealsStaleIndex(t *testing.T) {
	mem := newMem()
	ctx := context.Background()
	folder, _ := mem.EnsureFolder(ctx, "root-b")
	// Objeto existe no Drive, mas o índice aponta para um id que sumiu.
	if _, err := mem.Put(ctx, folder, objectName("k"), strings.NewReader("alive"), 5, "k", "text/plain"); err != nil {
		t.Fatal(err)
	}
	st, err := Open(Config{Path: filepath.Join(t.TempDir(), "index.db"), Root: "root", Blobs: mem})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.db.Exec(`INSERT INTO objects (bucket,key,drive_id,size,etag,content_type,modified) VALUES ('b','k','gone',0,'','','')`); err != nil {
		t.Fatal(err)
	}
	body, _, status, _, _, err := st.Get(ctx, "b", "k", 0, 0)
	if err != nil {
		t.Fatalf("esperava recuperação, veio %v", err)
	}
	defer body.Close()
	data, _ := io.ReadAll(body)
	if string(data) != "alive" || status != 200 {
		t.Fatalf("corpo=%q status=%d", data, status)
	}
}
