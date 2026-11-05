# gdrive2s3

S3-compatible gateway in Go backed by Google Drive. It serves `list`, `put`,
`get` with `Range`, `head` and `delete` with SigV4 authentication, with no
Cloudflare Worker in the path.

## Why

The original gateway is a Worker: 100 MiB body limit, per-request CPU limits and
an extra network hop. This binary runs under systemd, talks to Drive directly and
keeps a SQLite index so reads never walk folders.

## How it works

- An object becomes a Drive file named `obj-<sha256(key)>` inside the
  `FOLDER_ROOT-<bucket>` folder, with the original key in `appProperties`.
- The SQLite index (`objects`) stores key, Drive id, size, etag and content type.
  Listing and `head` are served from the index; only reads and writes touch Drive.
- `GET` with `Range` forwards the header to Drive and answers 206 with
  `Content-Range`.
- If the index loses a row, the read path recovers by querying Drive for the
  deterministic name.

## Configuration

| Variable | Description |
| --- | --- |
| `LISTEN` | local address (default `127.0.0.1:8787`) |
| `ACCESS_KEY` / `SECRET_KEY` | static S3 credentials |
| `REGION` | SigV4 scope region (default `auto`) |
| `BUCKETS` | allowed buckets, comma separated |
| `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REFRESH_TOKEN` | Drive OAuth |
| `INDEX_DB` | SQLite path (default `/var/lib/gdrive2s3/index.db`) |
| `FOLDER_ROOT` | Drive folder prefix (default `gdrive2s3`) |

## Run

```bash
go build -o gdrive2s3 ./cmd/gdrive2s3
LISTEN=127.0.0.1:8789 BUCKETS=archive ACCESS_KEY=... SECRET_KEY=... \
GOOGLE_CLIENT_ID=... GOOGLE_CLIENT_SECRET=... GOOGLE_REFRESH_TOKEN=... \
./gdrive2s3
```

## Tests and benchmarks

```bash
make test    # SigV4 against the official AWS signer, API, store and Drive
make bench   # Verify, GetRange and List1000
```

Local results (Apple M-series, Go 1.27):

| Benchmark | Time |
| --- | --- |
| `BenchmarkVerify` | ~6.5 µs |
| `BenchmarkGetRange` | ~8.8 µs |
| `BenchmarkList1000` | ~0.6 ms |

## Limitations

- No multipart: simple `PUT` with `Content-Length`. Enough for current media
  (largest object measured: 54 MiB) and for `rclone copy` below 200 MiB.
- One Drive file per object; renaming in Drive breaks the index, which recovers
  from the deterministic name.
- No versioning or lifecycle: expiration is the caller's responsibility.
