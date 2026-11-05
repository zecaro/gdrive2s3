// Package store keeps the object index in SQLite and delegates bytes to Drive.
// The index avoids folder walks on every read and is the source for S3 listing.
package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("object not found")

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
	mu      sync.Mutex
	folders map[string]string
}

func Open(path string, blobs Blobs, root string) (*Store, error) {
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
	return &Store{db: db, blobs: blobs, root: root, folders: map[string]string{}}, nil
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
