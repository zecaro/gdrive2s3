// g2s-import registers files that already exist in Drive into the gateway index.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/zecaro/gdrive2s3/internal/drive"
	"github.com/zecaro/gdrive2s3/internal/store"
)

func main() {
	var (
		folder  = flag.String("drive-folder", "", "Drive folder name that holds the objects")
		bucket  = flag.String("bucket", "", "bucket name to register")
		indexDB = flag.String("index", os.Getenv("INDEX_DB"), "SQLite index path")
		dryRun  = flag.Bool("dry-run", false, "walk and count without writing")
	)
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if *folder == "" || *bucket == "" {
		logger.Error("use -drive-folder and -bucket")
		os.Exit(2)
	}

	ctx := context.Background()
	client, err := drive.New(ctx, os.Getenv("GOOGLE_CLIENT_ID"), os.Getenv("GOOGLE_CLIENT_SECRET"), os.Getenv("GOOGLE_REFRESH_TOKEN"))
	if err != nil {
		logger.Error("drive", "err", err)
		os.Exit(1)
	}
	rootID, err := client.FolderIDByName(ctx, *folder)
	if err != nil {
		logger.Error("folder", "name", *folder, "err", err)
		os.Exit(1)
	}

	var entries []store.ImportEntry
	var walk func(id, prefix string) error
	walk = func(id, prefix string) error {
		children, err := client.ListFolder(ctx, id)
		if err != nil {
			return err
		}
		for _, child := range children {
			key := child.Name
			if prefix != "" {
				key = prefix + "/" + child.Name
			}
			if child.IsFolder {
				if err := walk(child.ID, key); err != nil {
					return err
				}
				continue
			}
			entries = append(entries, store.ImportEntry{Key: key, DriveID: child.ID, ContentType: child.MimeType, Size: child.Size})
		}
		return nil
	}
	if err := walk(rootID, ""); err != nil {
		logger.Error("walk", "err", err)
		os.Exit(1)
	}
	logger.Info("found", "objects", len(entries), "folder", *folder)

	if *dryRun {
		for i, entry := range entries {
			if i == 5 {
				break
			}
			fmt.Println(entry.Key)
		}
		return
	}

	st, err := store.Open(store.Config{Path: *indexDB, Root: "gdrive2s3", Blobs: client, MaxConcurrent: 4})
	if err != nil {
		logger.Error("index", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	inserted, err := st.Import(ctx, *bucket, entries)
	if err != nil {
		logger.Error("import", "err", err)
		os.Exit(1)
	}
	logger.Info("imported", "bucket", *bucket, "inserted", inserted, "total", len(entries))
}
