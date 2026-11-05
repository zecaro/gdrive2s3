#!/usr/bin/env bash
# Append-only backup of the media bucket to Google Drive.
#
# Modes:
#   incremental  copy objects younger than 3h (overlapping window)
#   full         copy everything missing; never --delete
set -euo pipefail

MODE="${1:-incremental}"
STATE_DIR=/var/lib/gdrive2s3
STATE_FILE="$STATE_DIR/backup.status"
mkdir -p "$STATE_DIR"

set -a
. /root/.rclone-backup.env
. /srv/tcsspy/bff-node.env
set +a

FLAGS=(--transfers 32 --checkers 64 --drive-chunk-size 32M --s3-no-check-bucket)
[ "$MODE" = "incremental" ] && FLAGS+=(--max-age 3h)

started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
if rclone copy "r2:$R2_BUCKET" gdrive:tcsspy-media-backup "${FLAGS[@]}" --log-level INFO; then
  objects=$(rclone lsf gdrive:tcsspy-media-backup --recursive --files-only 2>/dev/null | wc -l)
  printf '{"mode":"%s","ok":true,"finished":"%s","objects":%s}\n' "$MODE" "$started" "$objects" > "$STATE_FILE"
  echo "backup $MODE ok: $objects objects"
else
  printf '{"mode":"%s","ok":false,"finished":"%s"}\n' "$MODE" "$started" > "$STATE_FILE"
  if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
    curl -fsS -m 10 "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/sendMessage" \
      -d chat_id="$TELEGRAM_CHAT_ID" -d text="gdrive2s3 backup $MODE failed" >/dev/null || true
  fi
  exit 1
fi
