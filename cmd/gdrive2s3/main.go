// gdrive2s3 exposes a subset of the S3 API backed by Google Drive.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/zecaro/gdrive2s3/internal/drive"
	"github.com/zecaro/gdrive2s3/internal/s3api"
	"github.com/zecaro/gdrive2s3/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	listen := env("LISTEN", "127.0.0.1:8787")
	root := env("FOLDER_ROOT", "gdrive2s3")
	indexPath := env("INDEX_DB", "/var/lib/gdrive2s3/index.db")

	buckets := map[string]bool{}
	for _, name := range strings.Split(env("BUCKETS", ""), ",") {
		if name = strings.TrimSpace(name); name != "" {
			buckets[name] = true
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := drive.New(ctx, os.Getenv("GOOGLE_CLIENT_ID"), os.Getenv("GOOGLE_CLIENT_SECRET"), os.Getenv("GOOGLE_REFRESH_TOKEN"))
	if err != nil {
		logger.Error("drive: failed to start", "err", err)
		os.Exit(1)
	}
	st, err := store.Open(indexPath, client, root, os.Getenv("TMP_DIR"))
	if err != nil {
		logger.Error("index: failed to open", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// Abandoned multipart sessions hold temp files; drop them after a day.
	if removed, err := st.CleanupUploads(ctx, 24*time.Hour); err != nil {
		logger.Warn("multipart cleanup", "err", err)
	} else if removed > 0 {
		logger.Info("multipart cleanup", "removed", removed)
	}
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if removed, err := st.CleanupUploads(ctx, 24*time.Hour); err == nil && removed > 0 {
					logger.Info("multipart cleanup", "removed", removed)
				}
			}
		}
	}()

	server := s3api.New(st, s3api.Config{
		AccessKey: env("ACCESS_KEY", ""),
		SecretKey: env("SECRET_KEY", ""),
		Region:    env("REGION", "auto"),
		Buckets:   buckets,
		Logger:    logger,
	})

	httpServer := &http.Server{
		Addr:              listen,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 15 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	logger.Info("gdrive2s3 listening", "listen", listen, "buckets", len(buckets))
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server", "err", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
