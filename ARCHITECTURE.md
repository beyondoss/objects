# Beyond Objects Architecture

Takes HTTP requests (native REST or S3-compatible wire protocol), streams object bodies to/from GlideFS, and maintains an LSM-tree listing index for ordered prefix scans — all without buffering full objects into memory.

## Data Flow

### Write Path (PUT / PutObject / CompleteMultipart)

```
Client ──PUT /v1/{bucket}/{key}──► Auth middleware ──► objects::put()
                                        │
                                   401 Unauthorized
                                        │
                                        ▼
                               Stream body → .tmp/{uuid}
                               Compute MD5 while streaming
                               fsync
                               Set xattrs on temp file
                                        │
                              WriteCondition check
                           ┌───────────┴────────────┐
                     IfNoneMatch: *             IfMatch: "etag"
                     path.try_exists()          read xattr then compare
                           │                         │
                          412                       412
                           └─────────┬──────────────┘
                                     ▼
                              atomic rename → final path
                              fjall insert (spawn_blocking)
                              publish event to queue (best-effort)
                                     │
                                    200
```

### Read Path (GET / GetObject)

```
Client ──GET /v1/{bucket}/{key}──► Auth middleware
                                        │
                                   is object public?
                                   ┌────┴────┐
                                  yes        no
                                   │    verify token → 401
                                   └────┬────┘
                                        ▼
                                stat() + getxattr()
                                        │
                                  Range header?
                                ┌───────┴───────┐
                               yes              no
                              206 Partial      200 Full
                               └───────┬───────┘
                                       ▼
                                 sendfile() → client
```

### List Path (GET /v1/{bucket})

```
Client ──GET /v1/{bucket}?prefix=img/&cursor=img/b──►
          Auth → spawn_blocking → fjall prefix range scan
                                          │
                               collect limit+1 keys
                                          │
                              buffered head (64 concurrent)
                              stat() + getxattr() per key
                                          │
                               { objects, next_cursor }
```

### S3-Compatible Surface

```
S3 Client ──SigV4──► s3s fallback router
                          │
                   s3/auth.rs: access_key_id → token mapping
                   HMAC-SHA256(root_token, access_key_id) → secret
                          │
                   s3/handler.rs: S3 trait → native storage calls
                          │
                   same Storage / Index layer as REST
```

## Concepts & Terminology

| Term           | What It Controls                                                    | NOT                                                          |
| -------------- | ------------------------------------------------------------------- | ------------------------------------------------------------ |
| Root token     | Access to all buckets + bucket admin endpoints                      | A password that can be rotated independently per bucket      |
| Derived token  | Access to one named bucket only                                     | Stored anywhere — derived on every request                   |
| Bucket         | A directory under `OBJECTS_DATA_DIR`; all objects live under it     | A separate namespace with independent auth state             |
| Object key     | Relative path under the bucket directory (slashes create subdirs)   | A flat key — it maps to a real filesystem path               |
| WriteCondition | CAS guard evaluated atomically before rename                        | A database transaction                                       |
| Cursor         | Last key from previous page (exclusive lower bound in fjall scan)   | An opaque token — it is literally the key string             |
| Index          | fjall partition keyed by `"{bucket}\x00{key}"`                      | The authoritative store — filesystem is; index is derivative |
| Upload ID      | Directory name under `.multipart/` containing part files + metadata | A server-generated UUID with no other significance           |

## Core Mechanism

### Filesystem as the Database

Object existence is determined by file existence. There is no separate metadata store. Every object is a file at `{OBJECTS_DATA_DIR}/{bucket}/{key}`, and all metadata (etag, content-type, access level, user metadata) lives in extended attributes on that file. This means:

- `stat()` gives size, mtime
- `getxattr("user.etag")` gives the quoted MD5
- `getxattr("user.content-type")` gives MIME type
- `getxattr("user.access")` gives `"public"` or `"private"` (inherits from bucket xattr if absent)
- `getxattr("user.metadata")` gives a JSON blob

Atomicity comes from POSIX rename semantics: the temp file is written in full, fsynced, and only then renamed to the final path. A reader never sees a partial object.

### Listing Index (fjall)

`readdir` returns entries in hash order (on most filesystems), making prefix-scan pagination impossible. The fjall LSM-tree index at `OBJECTS_INDEX_DIR` maintains a sorted projection: keys `"{bucket}\x00{key}" → ""`. On startup, `index::reconcile()` walks the filesystem and inserts missing keys and removes stale entries to bring the index in sync with the filesystem.

List requests scan a prefix range with a cursor bound, fetch `limit + 1` entries to detect whether a next page exists, then concurrently `stat()` + `getxattr()` each key (up to 64 at once via `FuturesUnordered`). See `server/lib.rs:list_page()`.

### Auth — HMAC Token Derivation

```
root_token (env)
      │
      ├──► authenticate as root (all buckets + admin)
      │
      └── HMAC-SHA256(root_token, bucket_name) ──► hex ──► bucket-scoped token
```

Tokens are never stored. Verification is a constant-time compare using the `subtle` crate (`subtle::ConstantTimeEq`). For S3, `access_key_id` is either `"root"` or the bucket name; `secret_access_key` is `HMAC-SHA256(root_token, access_key_id)`.

Public objects (`user.access = "public"`) bypass auth on GET/HEAD entirely.

### Atomic Writes

1. Open `.tmp/{uuid}` (UUID v4 from `uuid` crate)
2. Stream body into it, accumulating MD5 via `md5` crate
3. `fsync` the temp file
4. Set all xattrs on the temp file
5. Evaluate WriteCondition (`IfNoneMatch`/`IfMatch`) against the target path
6. `fs::rename(.tmp/{uuid}, {bucket}/{key})` — atomic on POSIX

If the process crashes after step 4 but before step 6, the temp file is an orphan. `gc::gc_temp_files()` removes these on startup.

### Multipart Uploads

State is stored entirely on-disk under `.multipart/{upload_id}/`:

- `.meta.json` — bucket, key, content-type, access, user metadata, init timestamp
- `{part_n}` — raw bytes for each part (xattr `user.etag` = quoted MD5 of part)

`complete_multipart()` concatenates parts in the caller-supplied order into a new temp file, computes the S3-style multipart ETag (MD5 of concatenated part MD5s), then performs the same fsync → xattr → rename sequence as a regular write. The `.multipart/{upload_id}/` directory is removed after a successful rename.

## File Map

```
crates/
├── server/
│   ├── src/
│   │   ├── main.rs          jemalloc setup, process entry
│   │   ├── lib.rs           router builder, AppState, list_page()
│   │   ├── cli.rs           subcommand dispatch (serve / generate-openapi)
│   │   ├── config.rs        clap env config (all OBJECTS_* vars)
│   │   ├── error.rs         ApiError → HTTP status mapping
│   │   ├── telemetry.rs     OTLP tracer, JSON/pretty log format
│   │   ├── metrics.rs       Prometheus counters + histograms
│   │   ├── middleware/
│   │   │   └── auth.rs      Bearer token extraction + constant-time check
│   │   ├── routes/
│   │   │   ├── objects.rs   PUT/GET/HEAD/DELETE/PATCH/POST handlers
│   │   │   ├── buckets.rs   bucket CRUD (root-token only)
│   │   │   └── healthz.rs   /livez, /readyz
│   │   └── s3/
│   │       ├── handler.rs   S3 trait impl → storage calls
│   │       ├── auth.rs      SigV4 ↔ HMAC token mapping
│   │       ├── access.rs    bucket-scoped S3 access control
│   │       └── error.rs     S3 error ↔ ApiError
│   └── tests/
│       └── integration/     end-to-end tests against live server
├── storage/
│   ├── src/
│   │   ├── lib.rs           Storage struct (wraps data_dir path)
│   │   ├── types.rs         AccessLevel, ObjectInfo, BucketMeta, WriteCondition
│   │   ├── write.rs         write_object(), update_object_access()
│   │   ├── read.rs          head_object(), open_object(), delete_object(), copy_object(), move_object()
│   │   ├── bucket.rs        create/delete/list/get/update bucket
│   │   ├── multipart.rs     init/write/list/complete/abort multipart
│   │   ├── xattr.rs         getxattr/setxattr wrappers
│   │   ├── gc.rs            orphan temp file + stale multipart cleanup
│   │   └── error.rs         StorageError enum
└── index/
    └── src/
        └── lib.rs           Index struct (fjall), insert/delete/scan/reconcile

sdk/ts/
├── src/
│   ├── client.ts            ObjectsClient + bucket sub-client
│   ├── types.ts             generated from openapi/v1.json (openapi-typescript)
│   ├── errors.ts            ObjectsError class + stable error codes
│   └── utils/camelize.ts    snake_case → camelCase response transform
└── tests/
    └── *.test.ts            vitest suite against live Rust server
```

## On-Disk Layout

```
/data/                           ← OBJECTS_DATA_DIR
├── {bucket}/                    ← bucket (directory, xattr: user.access)
│   └── {key/path}              ← object (file, xattrs: etag, content-type, access, metadata)
├── .tmp/
│   └── {uuid}                  ← in-flight write staging (gc'd on startup if orphaned)
├── .multipart/
│   └── {upload_id}/
│       ├── .meta.json          ← bucket, key, content_type, access, user_metadata, init_time_secs
│       └── {part_n}            ← part bytes (xattr: user.etag)
└── .index/                      ← OBJECTS_INDEX_DIR (fjall database)
    └── ...                      ← LSM-tree files managed by fjall
```

## HTTP Routes

### Native REST (`/v1/`)

| Method | Path                    | Auth                        | Description                          |
| ------ | ----------------------- | --------------------------- | ------------------------------------ |
| PUT    | `/v1/{bucket}/{key...}` | bucket token                | Create or replace object             |
| GET    | `/v1/{bucket}/{key...}` | bucket token (public: none) | Download object                      |
| HEAD   | `/v1/{bucket}/{key...}` | bucket token (public: none) | Object metadata                      |
| DELETE | `/v1/{bucket}/{key...}` | bucket token                | Delete object                        |
| PATCH  | `/v1/{bucket}/{key...}` | bucket token                | Move (`{"key":"new"}`) or set access |
| POST   | `/v1/{bucket}/{key...}` | bucket token                | Copy (`{"source":"src/key"}`)        |
| GET    | `/v1/{bucket}`          | bucket token                | List objects (prefix + cursor)       |
| POST   | `/v1/buckets`           | root token                  | Create bucket                        |
| GET    | `/v1/buckets`           | root token                  | List buckets                         |
| GET    | `/v1/buckets/{name}`    | root token                  | Get bucket metadata                  |
| PATCH  | `/v1/buckets/{name}`    | root token                  | Update bucket config                 |
| DELETE | `/v1/buckets/{name}`    | root token                  | Delete bucket                        |
| GET    | `//v1/openapi.json`     | none                        | OpenAPI spec                         |
| GET    | `/livez`                | none                        | Liveness probe (process alive)       |
| GET    | `/readyz`               | none                        | Readiness probe (deps reachable)     |

### S3-Compatible (fallback, all explicit `/v1/*`, `/livez`, `/readyz` take priority)

ListBuckets · CreateBucket · DeleteBucket · HeadBucket · PutObject · GetObject · HeadObject · DeleteObject · CopyObject · ListObjectsV2 · CreateMultipartUpload · UploadPart · CompleteMultipartUpload · AbortMultipartUpload · ListMultipartUploads · ListParts

## State Machine — Multipart Upload

```
[client] ──CreateMultipartUpload──► created (.meta.json written)
                                          │
                                   UploadPart (any order, any count)
                                     part file written per call
                                          │
                                CompleteMultipartUpload
                               ┌──────────┴──────────┐
                           parts ok               parts missing/bad
                               │                         │
                        concat + rename               400 / 500
                        index insert                     │
                        rm -rf upload dir
                               │
                            visible
```

```
[client] ──AbortMultipartUpload──► rm -rf upload dir (any state)
```

| Event                   | Guard                      | What Actually Happens                                                                     |
| ----------------------- | -------------------------- | ----------------------------------------------------------------------------------------- |
| CreateMultipartUpload   | —                          | `.multipart/{uuid}/.meta.json` written; upload ID returned                                |
| UploadPart              | upload exists              | Part bytes streamed to `.multipart/{id}/{n}`; MD5 xattr set                               |
| CompleteMultipartUpload | all referenced parts exist | Parts concatenated in order → `.tmp/{uuid}` → fsync → xattrs → rename; upload dir removed |
| AbortMultipartUpload    | —                          | `.multipart/{id}/` removed unconditionally                                                |
| Startup GC              | orphan age                 | `.tmp/*` older than threshold removed; `.multipart/*` without recent activity removed     |

## Middleware Stack (innermost → outermost)

| Layer                     | Effect                                                      |
| ------------------------- | ----------------------------------------------------------- |
| `SetRequestIdLayer`       | Generates UUID `x-request-id`, attached to span             |
| `PropagateRequestIdLayer` | Echoes request ID in response headers                       |
| `TraceLayer`              | Structured span per request (method, path, status, latency) |
| `CatchPanicLayer`         | Catches Rust panics, returns 500                            |
| `DefaultBodyLimit(64 KB)` | Caps request body; **disabled** for object write routes     |
| `TimeoutLayer(30s)`       | Cancels slow requests; **disabled** for object write routes |

Object write routes (PUT, multipart part upload) remove both the body limit and the timeout. TCP keepalives handle dead connections on uploads.

## Trust Boundaries

**What the system verifies (rejects if invalid):**

- Bearer token matches root token or derived bucket token (constant-time compare)
- S3 SigV4 signature (via `s3s` crate)
- Object key does not contain path traversal components
- `Content-MD5` header matches computed MD5 when supplied
- WriteCondition (`If-None-Match`, `If-Match`) against live filesystem state

**What passes through unchecked:**

- Content of user metadata values (stored and returned verbatim)
- Object key beyond path traversal check (any valid UTF-8 path component is accepted)
- Authorization between buckets — a bucket token cannot access another bucket, but the check happens in middleware, not in Storage

**Why these boundaries:**

- The storage layer is intentionally thin; policy enforcement (auth, CAS) lives in the server crate so the storage crate can be used directly without a token system
- Public object access is checked before the auth middleware fires to avoid unnecessary HMAC computation on hot read paths

## Configuration

| Variable             | Default                 | What It Controls at Runtime                                       |
| -------------------- | ----------------------- | ----------------------------------------------------------------- |
| `OBJECTS_ROOT_TOKEN` | (required)              | Root auth token; also the HMAC key for derived tokens             |
| `OBJECTS_DATA_DIR`   | `/data`                 | Root directory for all bucket subdirectories and `.tmp/`          |
| `OBJECTS_INDEX_DIR`  | `/data/.index`          | Where fjall writes its LSM-tree files                             |
| `ADDRESS`            | `0.0.0.0:9000`          | Public bind address (REST + S3)                                   |
| `METRICS_ADDRESS`    | `127.0.0.1:9001`        | Internal Prometheus `/metrics` scrape endpoint                    |
| `LOG_LEVEL`          | `info`                  | `tracing` filter directive                                        |
| `OTLP_ENABLED`       | `false`                 | Whether to export traces to `OTLP_ENDPOINT`                       |
| `OTLP_ENDPOINT`      | `http://localhost:4317` | OTLP collector gRPC address                                       |
| `OBJECTS_URL`        | (none)                  | Public base URL for object URLs returned by SDK `client.url(key)` |
| `ENVIRONMENT`        | (none)                  | `development` enables pretty log output                           |

## Failure Modes

| Failure                     | What Actually Happens                                       | Recovery                                                       |
| --------------------------- | ----------------------------------------------------------- | -------------------------------------------------------------- |
| Process crash mid-write     | `.tmp/{uuid}` left on disk; object at final path unchanged  | `gc_temp_files()` on next startup removes orphans              |
| Process crash mid-multipart | `.multipart/{id}/` left on disk; final object unchanged     | `gc_multipart_uploads()` on next startup removes stale uploads |
| Index out of sync with FS   | Listing may miss objects or return deleted keys             | `index::reconcile()` on startup performs fsck; can be re-run   |
| fsync failure               | write_object returns StorageError; temp file left as orphan | Same as crash mid-write                                        |
| Queue publish failure       | Object is durably written; event is lost                    | Best-effort only; no retry                                     |
| Disk full                   | write_object fails during streaming; temp file removed      | 500 returned; no partial state visible                         |
| xattr not supported         | Startup will fail on first write attempt                    | GlideFS always supports xattrs; local ext4/apfs also work      |

## Why It Behaves This Way

### Why the filesystem is the database

Object data and metadata are co-located in one inode. Reads need no secondary lookup — `stat()` and `getxattr()` are a single syscall each. Atomic rename eliminates the window where readers could see partial state. GlideFS's COW semantics make this safe under concurrent writers.

### Why a separate listing index

POSIX `readdir` returns entries in hash order (not sorted), and there is no efficient "list keys starting after cursor X" primitive at the filesystem level. fjall provides a sorted LSM-tree that maps directly to prefix-scan pagination without sorting in memory.

### Why HMAC-derived tokens instead of stored tokens

Tokens are stateless: no token table to query, no join to compute access rights, and no additional storage. Bucket tokens can be distributed to clients without the root token ever leaving the server. Revocation requires rotating the root token (which rotates all derived tokens simultaneously).

### Why body limit and timeout are disabled for writes

A 64 KB body cap would prevent object uploads. A 30-second timeout would abort large uploads in flight. Both constraints are applied at the route level, not globally, so small-body endpoints (bucket CRUD, PATCH, etc.) retain the protections.

### Why events are best-effort

Storage operations are atomic and durable before the queue publish attempt. Making the write dependent on queue delivery would turn a transient queue outage into a storage outage. Consumers that need strong delivery guarantees can re-derive events from the storage state.
