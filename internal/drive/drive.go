// Package drive wraps the minimal Google Drive API used by the gateway:
// one folder per bucket, resumable upload, ranged download and delete.
package drive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/zecaro/gdrive2s3/internal/errs"
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// ErrNotFound aliases the shared sentinel.
var ErrNotFound = errs.ErrNotFound

type Client struct {
	svc     *drive.Service
	hc      *http.Client
	ts      oauth2.TokenSource
	apiBase string
}

func New(ctx context.Context, clientID, clientSecret, refreshToken string) (*Client, error) {
	conf := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Endpoint:     oauth2.Endpoint{TokenURL: "https://oauth2.googleapis.com/token"},
		Scopes:       []string{drive.DriveScope},
	}
	ts := conf.TokenSource(ctx, &oauth2.Token{RefreshToken: refreshToken})
	svc, err := drive.NewService(ctx, option.WithHTTPClient(oauth2.NewClient(ctx, ts)))
	if err != nil {
		return nil, err
	}
	return &Client{svc: svc, hc: oauth2.NewClient(ctx, ts), ts: ts, apiBase: "https://www.googleapis.com"}, nil
}

func (c *Client) EnsureFolder(ctx context.Context, name string) (string, error) {
	query := fmt.Sprintf("name = %q and mimeType = 'application/vnd.google-apps.folder' and trashed = false and 'root' in parents", name)
	list, err := c.svc.Files.List().Q(query).Fields("files(id)").PageSize(1).Context(ctx).Do()
	if err != nil {
		return "", err
	}
	if len(list.Files) > 0 {
		return list.Files[0].Id, nil
	}
	created, err := c.svc.Files.Create(&drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{"root"},
	}).Fields("id").Context(ctx).Do()
	if err != nil {
		return "", err
	}
	return created.Id, nil
}

// Put uploads the object under a deterministic name and keeps the original key in appProperties.
func (c *Client) Put(ctx context.Context, folderID, name string, body io.Reader, size int64, key, contentType string) (string, error) {
	file := &drive.File{
		Name:          name,
		Parents:       []string{folderID},
		MimeType:      contentType,
		AppProperties: map[string]string{"gdrive2s3-key": key},
	}
	call := c.svc.Files.Create(file).Fields("id").Context(ctx)
	// Cap the resumable chunk: ChunkSize(size) would allocate the whole object
	// in memory for a large file.
	const maxChunk = 8 << 20
	chunk := int64(maxChunk)
	if size > 0 && size < chunk {
		chunk = size
	}
	call = call.Media(body, googleapi.ChunkSize(int(chunk)))
	created, err := call.Do()
	if err != nil {
		return "", err
	}
	return created.Id, nil
}

func (c *Client) FindByName(ctx context.Context, folderID, name string) (string, int64, error) {
	query := fmt.Sprintf("name = %q and trashed = false and %q in parents", name, folderID)
	list, err := c.svc.Files.List().Q(query).Fields("files(id,size)").PageSize(1).Context(ctx).Do()
	if err != nil {
		return "", 0, err
	}
	if len(list.Files) == 0 {
		return "", 0, ErrNotFound
	}
	return list.Files[0].Id, list.Files[0].Size, nil
}

// OpenRange opens the content with Range and returns body, total and status (200/206/416).
func (c *Client) OpenRange(ctx context.Context, fileID string, offset, length int64) (io.ReadCloser, int64, int, error) {
	token, err := c.ts.Token()
	if err != nil {
		return nil, 0, 0, err
	}
	url := fmt.Sprintf("%s/drive/v3/files/%s?alt=media", c.apiBase, fileID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	if length > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	} else if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, 0, 0, ErrNotFound
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		resp.Body.Close()
		return nil, 0, http.StatusRequestedRangeNotSatisfiable, nil
	case resp.StatusCode >= 400:
		resp.Body.Close()
		return nil, 0, 0, fmt.Errorf("drive: status %d", resp.StatusCode)
	}
	total := resp.ContentLength
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if slash := strings.LastIndex(cr, "/"); slash >= 0 {
			fmt.Sscanf(cr[slash+1:], "%d", &total)
		}
	}
	return resp.Body, total, resp.StatusCode, nil
}

func (c *Client) Delete(ctx context.Context, fileID string) error {
	err := c.svc.Files.Delete(fileID).Context(ctx).Do()
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

func isNotFound(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusNotFound
	}
	return false
}
