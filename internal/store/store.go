// Package store keeps the object index in SQLite and delegates bytes to Drive.
// The index avoids folder walks on every read and is the source for S3 listing.
package store

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zecaro/gdrive2s3/internal/errs"

	_ "modernc.org/sqlite"
)

// ErrNotFound aliases the shared sentinel.
var ErrNotFound = errs.ErrNotFound

// Blobs is the byte backend; Drive implements it.
type Blobs interface {
	EnsureFolder(ctx context.Context, name string) (string, error)
	Put(ctx context.Context, folderID, name string, body io.Reader, size int64, key, contentType string) (string, error)
	FindByName(ctx context.Context, folderID, name string) (string, int64, error)
	OpenRange(ctx context.Context, fileID string, offset, length int64) (io.ReadCloser, int64, int, error)
	Delete(ctx context.Context, fileID string) error
}

type Object struct {
	Key         string
	Size        int64
	ETag        string
	ContentType string
	Modified    time.Time
}

type Store struct {
	db      *sql.DB
	blobs   Blobs
	root    string
	tmp     string
	mu      sync.Mutex
	folders map[string]string
}

func Open(path string, blobs Blobs, root, tmp string) (*Store, error) {
	if tmp == "" {
		tmp = path + ".parts"
	}
	if err := os.MkdirAll(tmp, 0o750); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS objects (
		bucket TEXT NOT NULL,
		key TEXT NOT NULL,
		drive_id TEXT NOT NULL,
		size INTEGER NOT NULL,
		etag TEXT NOT NULL,
		content_type TEXT NOT NULL,
		modified TEXT NOT NULL,
		PRIMARY KEY (bucket, key)
	) WITHOUT ROWID`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS multipart_uploads (
		id TEXT PRIMARY KEY,
		bucket TEXT NOT NULL,
		key TEXT NOT NULL,
		content_type TEXT NOT NULL,
		created_at TEXT NOT NULL
	)`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS multipart_parts (
		upload_id TEXT NOT NULL,
		part_number INTEGER NOT NULL,
		path TEXT NOT NULL,
		size INTEGER NOT NULL,
		etag TEXT NOT NULL,
		PRIMARY KEY (upload_id, part_number)
	)`); err != nil {
		return nil, err
	}
	return &Store{db: db, blobs: blobs, root: root, folders: map[string]string{}, tmp: tmp}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) folder(ctx context.Context, bucket string) (string, error) {
	s.mu.Lock()
	if id, ok := s.folders[bucket]; ok {
		s.mu.Unlock()
		return id, nil
	}
	s.mu.Unlock()
	id, err := s.blobs.EnsureFolder(ctx, s.root+"-"+bucket)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.folders[bucket] = id
	s.mu.Unlock()
	return id, nil
}

// objectName is the deterministic Drive file name: key hash, so it fits in
// 255 bytes and does not depend on folder hierarchy.
func objectName(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "obj-" + hex.EncodeToString(sum[:])
}

func (s *Store) Put(ctx context.Context, bucket, key string, body io.Reader, size int64, contentType string) (string, error) {
	folderID, err := s.folder(ctx, bucket)
	if err != nil {
		return "", err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	hash := md5.New()
	driveID, err := s.blobs.Put(ctx, folderID, objectName(key), io.TeeReader(body, hash), size, key, contentType)
	if err != nil {
		return "", err
	}
	etag := hex.EncodeToString(hash.Sum(nil))
	if _, err := s.db.Exec(`INSERT INTO objects (bucket,key,drive_id,size,etag,content_type,modified)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(bucket,key) DO UPDATE SET drive_id=excluded.drive_id,size=excluded.size,
		etag=excluded.etag,content_type=excluded.content_type,modified=excluded.modified`,
		bucket, key, driveID, size, etag, contentType, time.Now().UTC().Format(time.RFC3339)); err != nil {
		return "", err
	}
	return etag, nil
}

func (s *Store) lookup(ctx context.Context, bucket, key string) (driveID string, size int64, etag, contentType string, err error) {
	row := s.db.QueryRow(`SELECT drive_id,size,etag,content_type FROM objects WHERE bucket=? AND key=?`, bucket, key)
	err = row.Scan(&driveID, &size, &etag, &contentType)
	if err == nil {
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return
	}
	// Self-healing: a lost index row must not hide an object that exists.
	folderID, ferr := s.folder(ctx, bucket)
	if ferr != nil {
		return "", 0, "", "", ErrNotFound
	}
	id, sz, ferr := s.blobs.FindByName(ctx, folderID, objectName(key))
	if ferr != nil {
		return "", 0, "", "", ErrNotFound
	}
	_, _ = s.db.Exec(`INSERT INTO objects (bucket,key,drive_id,size,etag,content_type,modified)
		VALUES (?,?,?,?,?,?,?) ON CONFLICT(bucket,key) DO UPDATE SET drive_id=excluded.drive_id`,
		bucket, key, id, sz, "", "application/octet-stream", time.Now().UTC().Format(time.RFC3339))
	return id, sz, "", "application/octet-stream", nil
}

func (s *Store) Get(ctx context.Context, bucket, key string, offset, length int64) (io.ReadCloser, int64, int, string, string, error) {
	driveID, size, etag, contentType, err := s.lookup(ctx, bucket, key)
	if err != nil {
		return nil, 0, 0, "", "", err
	}
	body, total, status, err := s.blobs.OpenRange(ctx, driveID, offset, length)
	if errors.Is(err, ErrNotFound) {
		// The Drive file may have been renamed or removed; drop the stale row and
		// resolve the deterministic name again before answering 404.
		_, _ = s.db.Exec(`DELETE FROM objects WHERE bucket=? AND key=?`, bucket, key)
		if driveID, size, etag, contentType, err = s.lookup(ctx, bucket, key); err != nil {
			return nil, 0, 0, "", "", err
		}
		body, total, status, err = s.blobs.OpenRange(ctx, driveID, offset, length)
	}
	if err != nil {
		return nil, 0, 0, "", "", err
	}
	if total <= 0 {
		total = size
	}
	return body, total, status, etag, contentType, nil
}

func (s *Store) Stat(ctx context.Context, bucket, key string) (size int64, etag, contentType string, err error) {
	_, size, etag, contentType, err = s.lookup(ctx, bucket, key)
	return size, etag, contentType, err
}

func (s *Store) Delete(ctx context.Context, bucket, key string) error {
	driveID, _, _, _, err := s.lookup(ctx, bucket, key)
	if err != nil {
		return ErrNotFound
	}
	if err := s.blobs.Delete(ctx, driveID); err != nil {
		return err
	}
	_, err = s.db.Exec(`DELETE FROM objects WHERE bucket=? AND key=?`, bucket, key)
	return err
}

func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// List returns up to max objects from startAfter, filtered by prefix.
func (s *Store) List(ctx context.Context, bucket, prefix, startAfter string, max int) ([]Object, bool, error) {
	rows, err := s.db.Query(`SELECT key,size,etag,content_type,modified FROM objects
		WHERE bucket=? AND key LIKE ? ESCAPE '\' AND key > ?
		ORDER BY key LIMIT ?`, bucket, escapeLike(prefix)+"%", startAfter, max+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var objects []Object
	for rows.Next() {
		var o Object
		var modified string
		if err := rows.Scan(&o.Key, &o.Size, &o.ETag, &o.ContentType, &modified); err != nil {
			return nil, false, err
		}
		o.Modified, _ = time.Parse(time.RFC3339, modified)
		objects = append(objects, o)
	}
	truncated := len(objects) > max
	if truncated {
		objects = objects[:max]
	}
	return objects, truncated, rows.Err()
}

// Multipart fica em disco: cada parte é um arquivo temporário e o Complete
// concatena tudo num único envio ao Drive, sem carregar o objeto em memória.

type MultipartUpload struct {
	ID          string
	Bucket      string
	Key         string
	ContentType string
}

func (s *Store) CreateMultipart(bucket, key, contentType string) (string, error) {
	id := fmt.Sprintf("mpu-%d-%s", time.Now().UnixNano(), randomSuffix())
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	_, err := s.db.Exec(`INSERT INTO multipart_uploads (id,bucket,key,content_type,created_at) VALUES (?,?,?,?,?)`,
		id, bucket, key, contentType, time.Now().UTC().Format(time.RFC3339))
	return id, err
}

func randomSuffix() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano()%1e6)
	}
	return hex.EncodeToString(b[:])
}

func (s *Store) PutPart(uploadID string, partNumber int, body io.Reader) (string, error) {
	var bucket, key string
	if err := s.db.QueryRow(`SELECT bucket,key FROM multipart_uploads WHERE id=?`, uploadID).Scan(&bucket, &key); err != nil {
		return "", ErrNotFound
	}
	dir := filepath.Join(s.tmp, uploadID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	path := filepath.Join(dir, fmt.Sprintf("part-%05d", partNumber))
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	hash := md5.New()
	size, err := io.Copy(io.MultiWriter(file, hash), body)
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path)
		return "", err
	}
	etag := hex.EncodeToString(hash.Sum(nil))
	_, err = s.db.Exec(`INSERT INTO multipart_parts (upload_id,part_number,path,size,etag) VALUES (?,?,?,?,?)
		ON CONFLICT(upload_id,part_number) DO UPDATE SET path=excluded.path,size=excluded.size,etag=excluded.etag`,
		uploadID, partNumber, path, size, etag)
	if err != nil {
		return "", err
	}
	return etag, nil
}

type Part struct {
	Number int
	ETag   string
}

func (s *Store) CompleteMultipart(ctx context.Context, uploadID string, parts []Part) (string, error) {
	var bucket, key, contentType string
	if err := s.db.QueryRow(`SELECT bucket,key,content_type FROM multipart_uploads WHERE id=?`, uploadID).
		Scan(&bucket, &key, &contentType); err != nil {
		return "", ErrNotFound
	}
	if len(parts) == 0 {
		return "", errors.New("complete sem partes")
	}
	var (
		readers []io.Reader
		files   []*os.File
		total   int64
	)
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	for _, part := range parts {
		var path, etag string
		var size int64
		if err := s.db.QueryRow(`SELECT path,size,etag FROM multipart_parts WHERE upload_id=? AND part_number=?`,
			uploadID, part.Number).Scan(&path, &size, &etag); err != nil {
			return "", fmt.Errorf("parte %d ausente", part.Number)
		}
		if part.ETag != "" && strings.Trim(part.ETag, `"`) != etag {
			return "", fmt.Errorf("etag da parte %d não confere", part.Number)
		}
		file, err := os.Open(path)
		if err != nil {
			return "", err
		}
		files = append(files, file)
		readers = append(readers, file)
		total += size
	}
	etag, err := s.Put(ctx, bucket, key, io.MultiReader(readers...), total, contentType)
	if err != nil {
		return "", err
	}
	s.AbortMultipart(uploadID)
	return etag, nil
}

func (s *Store) AbortMultipart(uploadID string) error {
	if _, err := s.db.Exec(`DELETE FROM multipart_parts WHERE upload_id=?`, uploadID); err != nil {
		return err
	}
	if _, err := s.db.Exec(`DELETE FROM multipart_uploads WHERE id=?`, uploadID); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(s.tmp, uploadID))
}

func (s *Store) UploadExists(uploadID string) (MultipartUpload, error) {
	var out MultipartUpload
	err := s.db.QueryRow(`SELECT id,bucket,key,content_type FROM multipart_uploads WHERE id=?`, uploadID).
		Scan(&out.ID, &out.Bucket, &out.Key, &out.ContentType)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	return out, err
}

// CleanupUploads remove multipart sessions older than ttl, including their parts
// on disk, so abandoned uploads do not fill the volume.
func (s *Store) CleanupUploads(ctx context.Context, ttl time.Duration) (int, error) {
	cutoff := time.Now().Add(-ttl).UTC().Format(time.RFC3339)
	rows, err := s.db.Query(`SELECT id FROM multipart_uploads WHERE created_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		_ = s.AbortMultipart(id)
	}
	return len(ids), ctx.Err()
}
