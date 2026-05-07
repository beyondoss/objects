# Objects — Design Document

Object storage for the Beyond platform. An S3-compatible object storage service with a clean native SDK, built on GlideFS.

---

## Goals

- Store and retrieve arbitrary blobs (files, images, documents, etc.)
- Simple, idiomatic, ergonomic TypeScript SDK
- S3-compatible wire protocol for existing tooling (AWS CLI, framework adapters)
- Full CoW branching inherited from GlideFS — no extra work required
- Top-tier performance: zero-copy reads, streaming writes, no buffering

---

## Protocol surfaces

Two protocol surfaces, one storage backend — same pattern as Queue (native REST + SQS/SNS compat).

### Native REST (`/v1/`)

Clean JSON API, Beyond bearer-token auth, no XML, no Sig V4. This is what the TypeScript SDK uses.

### S3-compatible

Full S3 wire protocol via [`s3s`](https://github.com/s3s-project/s3s) — a hyper middleware that handles S3 routing, request parsing, multipart lifecycle, and error serialization. We implement one `S3` trait backed by the filesystem.

S3 "buckets" map directly to Beyond buckets on the filesystem. `CreateBucket` is a `mkdir` + xattr init. `ListBuckets` reads top-level directories.

---

## Storage

### Filesystem as the database

Objects are stored as files on a GlideFS-mounted volume at deterministic paths:

```
/data/{bucket}/{key...}
```

A key like `avatar.png` in bucket `images` lives at `/data/images/avatar.png`. No database lookup required to find a file.

The filesystem gives us for free:

- `stat().st_size` → object size
- `stat().st_mtime` → last modified

The only things we store beyond that are xattrs:

**Per-file:**

| xattr               | content                                                  |
| ------------------- | -------------------------------------------------------- |
| `user.etag`         | MD5 of object content (set on write)                     |
| `user.content-type` | MIME type                                                |
| `user.access`       | `public` or `private` (inherits from bucket if absent)   |
| `user.metadata`     | JSON blob of user-supplied headers (`x-amz-meta-*` etc.) |

**Per-bucket directory:**

| xattr         | content                                                    |
| ------------- | ---------------------------------------------------------- |
| `user.access` | `public` or `private` (default for objects in this bucket) |

GET is: `stat()` + `getxattr()` + `sendfile()`. No network round-trips.

### Buckets

A bucket is a top-level directory. It exists when the directory exists. Its config lives as xattrs on the directory itself — no sidecar files, no separate config store.

```
/data/
  default/    ← default bucket (OBJECTS_ROOT_TOKEN env var)
  images/     ← scoped bucket (IMAGES_KEY env var)
  documents/  ← scoped bucket (DOCUMENTS_KEY env var)
```

Creating a bucket = `mkdir` + `setxattr(token_hash, access)`. Deleting a bucket = `rmdir`. Listing buckets = `readdir` on `/data/`.

**Token auth**: bucket tokens are derived from the `OBJECTS_ROOT_TOKEN` via HMAC:

```
bucket_token = HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket_name)
```

The service validates by recomputing `HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket)` on every request and comparing to the presented bearer token. No per-bucket secret storage — just the root token in env. Rotating `OBJECTS_ROOT_TOKEN` rotates all bucket tokens.

The default bucket validates against `OBJECTS_ROOT_TOKEN` directly.

### Listing index — fjall

`readdir()` on ext4/XFS returns entries in hash order, not alphabetical — useless for S3-style prefix scans with cursor pagination. We maintain a [fjall](https://github.com/fjall-rs/fjall) LSM-tree index alongside the filesystem, storing **keys only** per bucket. Nothing else — no size, no etag, no mtime. Those come from the filesystem. One entry per stored object:

```
bucket:      "images"
key:         "avatar.png"
value:       (empty — presence is the index)
```

Prefix range scans via fjall's range iterator. Cursor pagination by seeking to the last seen key.

**Why fjall for GlideFS:** fjall writes are sequential (LSM append-only), which maps cleanly to GlideFS's 128 KB block packing — no random writes, no sub-block scatter. At per-tenant dataset sizes (tens of thousands of keys), the index fits mostly in fjall's memtable and compaction barely triggers, so post-fork block divergence is nearly theoretical.

### Write path

```
1. Authenticate: HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket) == bearer token
2. Stream body to temp path (.tmp/{uuid})
3. Compute ETag (MD5) while streaming
4. fsync
5. setxattr (etag, content-type, access, user metadata)
6. rename() temp → final path   ← atomic
7. fjall INSERT key
```

`rename()` is atomic on POSIX — the object either appears at its final path or doesn't. It only becomes visible in listings after step 7.

**Crash recovery**: on startup, scan the filesystem and insert any keys missing from the fjall index. Orphaned temp files (crash during step 2-4) are GC'd after a threshold.

### Conditional writes (CAS)

Supported via standard HTTP conditional headers on PUT:

- `If-None-Match: *` — only write if the object does not exist. Implemented via `O_CREAT | O_EXCL` on temp file creation — atomic at the OS level.
- `If-Match: "<etag>"` — only write if the current ETag matches. Implemented by reading `user.etag` xattr and comparing before rename.

### Multipart uploads (S3-compat only)

Multipart is not exposed in the native API — streaming handles large uploads directly. The S3-compatible layer supports it for clients that require it. Parts written to `.multipart/{upload_id}/{part_number}`; assembled on `CompleteMultipartUpload` via sequential concatenation → `fsync` → `rename()` → fjall INSERT.

---

## Access control

**One root token, bucket tokens derived via HMAC.** The platform gives you `OBJECTS_ROOT_TOKEN`. Bucket tokens are `HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket_name)` — deterministic, no extra storage.

```ts
import { createObjectsClient, deriveToken } from "@beyond.dev/objects";

// root token — full access to default bucket
const objects = createObjectsClient();
// reads OBJECTS_URL + OBJECTS_ROOT_TOKEN

// derive a scoped token to hand off to a specific service (HMAC-SHA256, async)
const imagesToken = await deriveToken(process.env.OBJECTS_ROOT_TOKEN, "images");
const images = createObjectsClient({ bucket: "images", token: imagesToken });
```

**Per-object visibility**, set at write time, inherited from bucket default if absent:

- `public` — no token required on GET. Response includes `Access-Control-Allow-Origin: *`.
- `private` (default) — token required.

Writes always require a valid token regardless of visibility.

**No presigned URLs, no client upload tokens.** Temporary links are an application-layer concern.

---

## URLs

Each project gets its own service instance and GlideFS volume:

```
https://objects.{project}.beyond.page/{bucket}/{key...}
```

The base URL is provided via `OBJECTS_URL` env var. Custom domains are handled at the platform proxy level.

---

## TypeScript SDK

The SDK mirrors the Queue/KV/Auth pattern: every method returns a discriminated `{ data, error, response }` tuple. Errors are values, not exceptions.

```ts
import {
  createObjectsClient,
  deriveToken,
  ObjectsError,
} from "@beyond.dev/objects";

// default bucket — reads OBJECTS_URL + OBJECTS_ROOT_TOKEN
const objects = createObjectsClient();

// scoped to a named bucket with a derived token
const imagesToken = await deriveToken(process.env.OBJECTS_ROOT_TOKEN, "images");
const images = createObjectsClient({ bucket: "images", token: imagesToken });

// Upload
const { data, error, response } = await images.put("avatar.png", file, {
  contentType: "image/png",
  access: "public",
});
if (error) throw error; // or branch on error.code
console.log(data.url, data.etag, data.size);

// Conditional writes (CAS)
await objects.put("jobs/lock", payload, { ifNoneMatch: "*" });
await objects.put("config.json", updated, {
  ifMatch:
    "\"d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35\"",
});

// Download — `data` is a ReadableStream<Uint8Array>
const { data: stream } = await images.get("avatar.png");

// Range requests (returns 206; Content-Range is on `response.headers`)
await images.get("video.mp4", { range: { start: 0, end: 1023 } });
await images.get("video.mp4", { range: { suffix: 4096 } }); // last 4 KiB

// User metadata — round-trips as `x-amz-meta-*` headers
await images.put("avatar.png", file, {
  metadata: { owner: "u_123", traceId: "abc" },
});

// Metadata
const { data: meta } = await images.head("avatar.png");
// meta = { size, etag, contentType, access, lastModified, metadata }

// Delete (idempotent — 404 returns no error)
await images.delete("avatar.png");

// Move + copy (server-side, within same bucket)
await images.move("original.jpg", "archived/original.jpg");
await images.copy("original.jpg", "thumbnail.jpg");

// Update access without moving
await images.setAccess("avatar.png", "public");

// List (prefix scan, cursor pagination)
const { data: page } = await images.list({ prefix: "avatars/" });
for (const o of page.objects) console.log(o.key, o.url);
if (page.nextCursor) {
  await images.list({ prefix: "avatars/", cursor: page.nextCursor });
}

// Bucket admin (root-token only)
await objects.buckets.create("images", { access: "private" });
await objects.buckets.update("images", { access: "public" });
await objects.buckets.list();
await objects.buckets.delete("images");

// URL builder — pure construction, no I/O
const src = images.url("avatar.png");
// → https://objects.my-project.beyond.page/v1/images/avatar.png
```

### Client options

| Option       | Type       | Default                          | Description                              |
| ------------ | ---------- | -------------------------------- | ---------------------------------------- |
| `url`        | `string`   | `process.env.OBJECTS_URL`        | Base URL of the beyond-objects server    |
| `token`      | `string`   | `process.env.OBJECTS_ROOT_TOKEN` | Bearer token (root, or derived)          |
| `bucket`     | `string`   | `"default"`                      | Bucket this client operates on           |
| `fetch`      | `function` | `globalThis.fetch`               | Custom fetch (for pooling or test mocks) |
| `timeout`    | `number`   | —                                | Per-request timeout in milliseconds      |
| `retries`    | `number`   | `2`                              | Max retries on transient 5xx failures    |
| `onRequest`  | `function` | —                                | Called before each request               |
| `onResponse` | `function` | —                                | Called after each response with duration |

### What `list()` returns

```ts
{
  objects: [
    {
      key: 'avatar.png',
      size: 48291,
      etag: '"d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"',
      contentType: 'image/png',
      access: 'public',
      lastModified: '2026-05-07T12:00:00Z',
      url: 'https://objects.my-project.beyond.page/v1/images/avatar.png',
    },
  ],
  nextCursor: 'avatar2.png', // pass as cursor to next list() call; absent when done
}
```

### Errors

Non-2xx responses populate the `error` field with an `ObjectsError`. The class shape mirrors Queue:

```ts
class ObjectsError extends Error {
  readonly code: string; // e.g. "object_not_found", "etag_mismatch"
  readonly status: number; // HTTP status
  readonly hint?: string; // optional actionable guidance
}
```

`code` is the stable contract. The full enum is `unauthorized | forbidden | object_not_found | bucket_not_found | bucket_not_empty | object_exists | etag_mismatch | invalid_key | bad_request | range_not_satisfiable | internal_error`.

---

## Native REST API

```
PUT    /v1/{bucket}/{key...}               Upload (streaming, atomic rename)
GET    /v1/{bucket}/{key...}               Download (sendfile, range requests)
HEAD   /v1/{bucket}/{key...}               Metadata only
DELETE /v1/{bucket}/{key...}               Delete
PATCH  /v1/{bucket}/{key...}               Move/rename { key: "dest/key" } or update metadata { access: "public" }
POST   /v1/{bucket}/{key...}               Copy { source: "src/key" }
GET    /v1/{bucket}?prefix=&cursor=        List (fjall range scan)

POST   /v1/buckets                         Create bucket { name, access }
GET    /v1/buckets                         List buckets
GET    /v1/buckets/{name}                  Get bucket config
PATCH  /v1/buckets/{name}                  Update config { access }
DELETE /v1/buckets/{name}                  Delete bucket
```

---

## Events

Object mutations emit events to a Beyond Queue. This isn't just a convenience feature — it's the composition principle that makes Beyond's branching story real for reactive workloads.

When you `glide fork`, the objects volume forks _and_ the queue forks with it. An image upload pipeline — user uploads → `objects:put` event → queue consumer resizes thumbnails — runs correctly in a preview branch, against real production data, fully isolated. No other platform can offer that: Vercel preview deploys don't fork S3 or SQS; AWS has no branching concept at all.

Configure via `OBJECTS_EVENTS_QUEUE_URL` — if absent, events are silently skipped (zero overhead on the hot path).

**Event types**: `objects:put`, `objects:delete`, `objects:copy`, `objects:move`

**Payload**:

```json
{
  "event": "objects:put",
  "bucket": "images",
  "key": "avatar.png",
  "size": 48291,
  "etag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
  "timestamp": "2026-05-06T12:00:00Z"
}
```

Published best-effort after the operation commits (post-rename + fjall INSERT). A queue failure never fails the storage operation — log at `warn` and continue.

**TypeScript SDK**:

```ts
// subscribing is just using the Queue SDK — objects doesn't own this
import { createQueueClient } from "@beyond.dev/queue";

const queue = createQueueClient();
queue.subscribe(process.env.OBJECTS_EVENTS_QUEUE_URL, (event) => {
  if (event.event === "objects:put" && event.key.startsWith("avatars/")) {
    // ...
  }
});
```

---

## Open questions

- **`put()` value types** — overloaded: `string | Buffer | Uint8Array | ReadableStream`. All funnel to the same streaming write path server-side.

---

## Project structure

```
objects/
  Cargo.toml                  # workspace root
  crates/
    server/                   # main binary — Axum + s3s, routes, auth middleware
    storage/                  # filesystem I/O: streaming write, sendfile, xattrs, GC
    index/                    # fjall listing index: insert, delete, scan, reconcile
  bench/                      # criterion benchmarks
  xtask/                      # build-time tasks (generate-openapi)
  openapi/
    v1.json                   # generated OpenAPI spec
  sdk/
    ts/
      package.json
      tsconfig.json
      tsdown.config.ts
      vitest.config.ts
      .npmrc
      scripts/
        generate-types.mjs
      src/
        index.ts
        client.ts
        types.ts              # generated from openapi/v1.json
        error.ts
  .github/
    workflows/
      ci.yml
      release-sdk.yml
      release-api.yml
  mise.toml
  dprint.json
```

---

## Rust

### Cargo workspace

```toml
[workspace]
members = ["crates/server", "crates/storage", "crates/index", "bench", "xtask"]
resolver = "2"

[workspace.package]
edition = "2024"

[profile.release]
lto = true
codegen-units = 1
panic = "abort"
strip = true
```

### Key crates

| crate                                  | version                 | purpose                                 |
| -------------------------------------- | ----------------------- | --------------------------------------- |
| `axum`                                 | 0.8                     | HTTP server                             |
| `tokio`                                | 1 (full)                | async runtime                           |
| `tower`                                | 0.5                     | middleware                              |
| `tower-http`                           | 0.6                     | trace, request-id, catch-panic, timeout |
| `s3s`                                  | latest                  | S3 wire protocol middleware             |
| `s3s-fs`                               | latest                  | reference for S3 trait impl             |
| `fjall`                                | latest                  | listing index (LSM, append-only)        |
| `serde` + `serde_json`                 | 1                       | serialization                           |
| `utoipa`                               | 5 (axum_extras, chrono) | OpenAPI spec generation                 |
| `thiserror`                            | 2                       | error types                             |
| `anyhow`                               | 1                       | error propagation                       |
| `uuid`                                 | 1 (v4, serde)           | upload IDs, temp file names             |
| `chrono`                               | 0.4 (serde)             | timestamps                              |
| `tracing`                              | 0.1                     | structured logging                      |
| `tracing-subscriber`                   | 0.3 (env-filter, json)  | log output                              |
| `opentelemetry` + `opentelemetry-otlp` | 0.31                    | OTLP tracing export                     |
| `tracing-opentelemetry`                | 0.32                    | bridge                                  |
| `prometheus`                           | 0.14                    | metrics                                 |
| `tikv-jemallocator`                    | 0.6                     | allocator                               |
| `clap`                                 | 4 (derive)              | CLI                                     |
| `md-5`                                 | 0.10                    | ETag computation                        |
| `hmac` + `sha2`                        | 0.12                    | bucket token derivation                 |
| `reqwest`                              | 0.13                    | HTTP client (health checks, etc.)       |
| `testcontainers`                       | 0.27                    | integration tests                       |

### OpenAPI with utoipa

Each handler is annotated with `#[utoipa::path(...)]`. The root `ApiDoc` struct assembles the full spec:

```rust
#[derive(OpenApi)]
#[openapi(
    info(title = "Beyond Objects", version = "1"),
    modifiers(&BearerAuth),
    paths(
        objects::put_object,
        objects::get_object,
        objects::head_object,
        objects::delete_object,
        objects::move_object,
        objects::copy_object,
        objects::list_objects,
        buckets::create_bucket,
        buckets::list_buckets,
        buckets::get_bucket,
        buckets::update_bucket,
        buckets::delete_bucket,
    ),
    components(schemas(
        ErrorResponse,
        PutObjectRequest,
        ListObjectsResponse,
        ObjectItem,
        CreateBucketRequest,
        BucketResponse,
        // ...
    )),
    tags(
        (name = "objects", description = "Object operations"),
        (name = "buckets", description = "Bucket management"),
    )
)]
pub struct ApiDoc;
```

Spec served at `GET /v1/openapi.json`. The `xtask` crate runs the binary with `generate-openapi` to write `openapi/v1.json`.

### Error response shape

Consistent with auth and queue:

```rust
#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Serialize, ToSchema)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}
```

Errors implement `IntoResponse`. Internal errors (5xx) are logged at `error` level before the opaque response is returned — the client never sees internal details.

### Allocator

```rust
// crates/server/src/main.rs
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

Matches auth and queue — jemalloc on every non-MSVC target.

### Middleware stack

```rust
let router = Router::new()
    // ... routes ...
    .layer(
        ServiceBuilder::new()
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
            .layer(TraceLayer::new_for_http())
            .layer(TimeoutLayer::new(Duration::from_secs(30)))
            .layer(CatchPanicLayer::new()),
    );
```

Order matches auth and queue exactly: request-id → trace → timeout (30 s, 408) → catch-panic.

`DefaultBodyLimit` set separately on upload routes only (or removed entirely for streaming uploads).

Metrics registered on a separate `metrics_router` (not part of the main app router) for internal-only exposure.

### Config / CLI

```rust
#[derive(clap::Parser)]
pub struct Config {
    #[arg(long, env = "OBJECTS_ROOT_TOKEN")]
    pub objects_root_token: Secret<String>,

    #[arg(long, env = "OBJECTS_DATA_DIR", default_value = "/data")]
    pub data_dir: PathBuf,

    #[arg(long, env = "OBJECTS_PORT", default_value = "9000")]
    pub port: u16,

    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otel_endpoint: Option<String>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("objects_root_token", &"[redacted]")
            .field("data_dir", &self.data_dir)
            .field("port", &self.port)
            .field("otel_endpoint", &self.otel_endpoint)
            .finish()
    }
}
```

All config via `clap` derive with `env = "VAR"`. Secrets always redacted in `Debug`.

### Observability

```rust
fn init_tracing(otel_endpoint: Option<&str>) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer().with_target(true);

    if std::env::var("RUST_LOG_FORMAT").as_deref() == Ok("json") {
        // production: JSON
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer.json())
            .with(otel_layer(otel_endpoint))
            .init();
    } else {
        // development: human-readable pretty
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer.pretty())
            .init();
    }
}
```

`RUST_LOG` controls verbosity. `RUST_LOG_FORMAT=json` in production. OTLP export is optional — no-ops when `otel_endpoint` is absent.

Prometheus metrics exposed at `/metrics` on the internal metrics port (not the main API port).

### Health check

```
GET /healthz
→ 200 { "status": "ok", "version": "0.1.0" }
→ 503 { "status": "degraded", "version": "0.1.0" }
```

Matches auth and queue. Checks that the data directory and fjall index are reachable. Version injected via `env!("CARGO_PKG_VERSION")`.

### xtask: generate-openapi

```
cargo run -p xtask -- generate-openapi
```

Subcommand in `xtask/src/main.rs`. Starts the axum router, calls `ApiDoc::openapi()`, serializes to `openapi/v1.json`. No network, no side effects — pure spec extraction. Matches queue's xtask pattern.

### Integration tests

```rust
// tests/common/mod.rs
static SERVER: OnceLock<TestServer> = OnceLock::new();

pub fn server() -> &'static TestServer {
    SERVER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let server = TestServer::start().await;
                tx.send(server).unwrap();
                tokio::signal::ctrl_c().await.ok();
            });
        });
        rx.recv().unwrap()
    })
}
```

`testcontainers` for external deps (none in objects — storage is local). `OnceLock` singleton started in a `thread::spawn` + blocking tokio runtime. Tests call `server()` and get a `TestClient` with the base URL. Matches the auth and queue pattern exactly.

---

## Formatting — dprint

Identical config across all Beyond services. Plugins: `typescript`, `json`, `markdown`, `toml` (native); `rustfmt` and `yamlfmt` via exec.

```jsonc
// dprint.jsonc
{
  "plugins": [
    "https://plugins.dprint.dev/typescript-0.93.2.wasm",
    "https://plugins.dprint.dev/json-0.19.4.wasm",
    "https://plugins.dprint.dev/markdown-0.17.8.wasm",
    "https://plugins.dprint.dev/toml-0.6.3.wasm",
    {
      "name": "rustfmt",
      "path": "rustfmt",
      "fileExtensions": ["rs"],
    },
    {
      "name": "yamlfmt",
      "path": "yamlfmt",
      "fileExtensions": ["yml", "yaml"],
      "args": ["-in", "{{file_path}}"],
    },
  ],
  "includes": ["**/*.{ts,tsx,js,json,jsonc,md,toml,rs,yml,yaml}"],
  "excludes": ["**/node_modules", "**/dist", "**/target", "**/.git"],
}
```

---

## TypeScript SDK

### package.json

```json
{
  "name": "@beyond.dev/objects",
  "version": "0.1.0-dev",
  "type": "module",
  "engines": { "node": ">=18" },
  "files": ["dist"],
  "exports": {
    ".": { "import": "./dist/index.js", "types": "./dist/index.d.ts" }
  },
  "scripts": {
    "build": "tsdown",
    "typecheck": "tsc --noEmit",
    "test": "vitest run",
    "test:watch": "vitest"
  },
  "dependencies": {
    "openapi-fetch": "^0.17.0"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "openapi-typescript": "^7.6.1",
    "tsdown": "^0.21.10",
    "typescript": "^6.0.3",
    "vitest": "^4.1.5"
  }
}
```

### tsconfig.json

```json
{
  "compilerOptions": {
    "target": "ESNext",
    "module": "Preserve",
    "moduleResolution": "Bundler",
    "lib": ["ESNext", "DOM"],
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "noImplicitReturns": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "erasableSyntaxOnly": true,
    "isolatedModules": true,
    "verbatimModuleSyntax": true,
    "outDir": "dist",
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "skipLibCheck": true
  },
  "include": ["src"],
  "exclude": ["dist", "node_modules"]
}
```

### .npmrc

```
legacy-peer-deps=true
```

### Type generation pipeline

1. `mise run generate:openapi` → runs `cargo run -p xtask -- generate-openapi` → writes `openapi/v1.json`
2. `mise run generate:types` → runs `sdk/ts/scripts/generate-types.mjs` → writes `sdk/ts/src/types.ts` via `openapi-typescript`
3. `sdk/ts/src/types.ts` exports `components`, `paths`, `operations` — consumed internally by `client.ts` via `openapi-fetch`

### SDK exports

```ts
// src/index.ts
export { createObjectsClient, deriveToken } from "./client.js";
export type {
  HeadResult,
  ListOptions,
  ListResult,
  ObjectItem,
  ObjectsClient,
  PutOptions,
} from "./client.js";
export { ObjectsError } from "./error.js";
export type { components, operations, paths } from "./types.js";
```

### vitest.config.ts

```ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    globalSetup: ["./src/test/setup.ts"],
    testTimeout: 30_000,
    hookTimeout: 60_000,
    forceExit: true,
  },
});
```

Matches auth and queue. `globalSetup` starts the test server once. `forceExit` handles the background thread.

### Response transformation — camelize

API responses (snake_case JSON) are transformed to camelCase in the SDK layer using a recursive utility:

```ts
// src/camelize.ts — identical across all Beyond SDKs
type Camelize<T> = T extends (infer U)[] ? Camelize<U>[]
  : T extends object
    ? { [K in keyof T as CamelCase<string & K>]: Camelize<T[K]> }
  : T;

export function camelize<T>(obj: T): Camelize<T> {/* ... */}
export function snakenize<T>(obj: T): Snakenize<T> {/* ... */} // for outgoing request bodies
```

Request bodies (camelCase → snake_case) use `snakenize` before serialization. Shared utility — copy from `@beyond.dev/kv` or `@beyond.dev/queue`.

---

## mise tasks

```toml
[tools]
dprint = "latest"
node = "lts"
rust = { version = "1.92", components = "rustfmt,clippy", targets = "aarch64-unknown-linux-gnu,x86_64-unknown-linux-gnu" }
yamlfmt = "latest"
cargo-binstall = "latest"
"cargo:cross" = "latest"

[tasks."build:rs"]
run = "cargo build"

[tasks."build:rs:release"]
run = "cargo build --release"

[tasks."build:ts"]
run = "npm run build"
dir = "sdk/ts"
depends = ["generate:types"]

[tasks."check:rs"]
run = "cargo clippy --workspace -- -D warnings"

[tasks."check:ts"]
run = "npm run typecheck"
dir = "sdk/ts"
depends = ["generate:types"]

[tasks."check:fmt"]
run = "dprint check"

[tasks.format]
run = "dprint fmt"

[tasks."install:ts"]
run = "npm ci"
dir = "sdk/ts"

[tasks."test:unit:rs"]
run = "cargo test --lib"

[tasks."test:integration:rs"]
run = "cargo test --test integration"

[tasks."test:integration:ts"]
run = "npm test"
dir = "sdk/ts"
depends = ["generate:types", "build:rs"]

[tasks."generate:openapi"]
run = "cargo run -p xtask -- generate-openapi"

[tasks."generate:types"]
run = "node scripts/generate-types.mjs"
dir = "sdk/ts"
depends = ["install:ts", "generate:openapi"]
```

---

## GitHub workflows

### ci.yml

Triggers: `push` to main, `pull_request` to main.

```yaml
jobs:
  ci:
    runs-on: ubuntu-latest
    steps:
      - mise run check:fmt
      - mise run check:rs
      - mise run test:unit:rs
      - mise run test:integration:rs
      - mise run check:ts
      - mise run build:rs:release
      - mise run build:ts
      - mise run test:integration:ts
      - cargo audit
  generate-check:
    runs-on: ubuntu-latest
    steps:
      - mise run generate:openapi
      - mise run generate:types
      - git diff --exit-code openapi/v1.json sdk/ts/src/types.ts
```

### release-sdk.yml

Triggers: `push` tags matching `sdk/v*`.

Publishes `@beyond.dev/objects` to npm. Version extracted from tag: `VERSION=${GITHUB_REF_NAME#sdk/v}`.

### release-api.yml

Triggers: `push` tags matching `api-v*`.

Matrix: `ubuntu-latest` (amd64) + `ubuntu-24.04-arm` (arm64). Builds release binary, packages as `beyond-objects-v${VERSION}-linux-${arch}.tar.gz`, creates GitHub release.

---

## Performance notes

- **Uploads**: body streamed directly to disk — no full-object buffering in memory
- **Downloads**: `sendfile()`/`splice()` for zero-copy transfer
- **GET hot path**: `stat()` + `getxattr()` + `sendfile()` — no DB, no network
- **Auth**: `HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket)` compared to bearer token — pure computation, no I/O
- **Listing**: fjall range scan + filesystem `stat()` per result for size/mtime; both are local syscalls (microseconds each)
- **GlideFS alignment**: sequential writes coalesce into 128 KB blocks naturally; sequential readahead kicks in automatically for large reads
