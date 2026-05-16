# beyond/objects

An S3-compatible object store backed by the local filesystem. Native REST API and full S3 wire protocol — no database, no buffering, authentication via HMAC-derived tokens.

Objects are files on disk. Metadata lives in extended attributes. Writes are atomic: stream to a temp file, fsync, set xattrs, rename. Reads stream directly to the client without buffering the full body into memory.

## Quick Start

**Put and get an object:**

```sh
curl -T ./photo.jpg \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: image/jpeg" \
  http://localhost:9000/v1/default/photos/cat.jpg

curl -H "Authorization: Bearer secret" \
  http://localhost:9000/v1/default/photos/cat.jpg \
  -o cat.jpg
```

**Or use the TypeScript SDK:**

```sh
npm install @beyond.dev/objects
```

```ts
import { objects } from "@beyond.dev/objects";

await objects.put("photos/cat.jpg", file, { contentType: "image/jpeg" });
const stream = await objects.get("photos/cat.jpg");
await objects.delete("photos/cat.jpg");
await objects.close();
```

## Operations

| Operation  | HTTP                                                |
| ---------- | --------------------------------------------------- |
| Upload     | `PUT /v1/{bucket}/{key}`                            |
| Download   | `GET /v1/{bucket}/{key}`                            |
| Metadata   | `HEAD /v1/{bucket}/{key}`                           |
| Delete     | `DELETE /v1/{bucket}/{key}`                         |
| Move       | `PATCH /v1/{bucket}/{key}` + `{"key": "new/key"}`   |
| Copy       | `POST /v1/{bucket}/{key}` + `{"source": "src/key"}` |
| Set access | `PATCH /v1/{bucket}/{key}` + `{"access": "public"}` |
| List       | `GET /v1/{bucket}?prefix=photos/&limit=100&cursor=` |

**Conditional writes** — `If-None-Match: *` rejects if the key exists; `If-Match: <etag>` rejects if the ETag doesn't match. Both are atomic.

**Public objects** — upload with `X-Beyond-Access: public` (or `{ access: "public" }` in the SDK) to make a key readable without a token.

**Byte ranges** — `GET` supports the `Range` header for partial downloads.

## S3 Compatible

Any S3 client works. Derive credentials from the root token — no separate credential store:

```ts
import { deriveS3Credentials } from "@beyond.dev/objects";

const { accessKeyId, secretAccessKey } = deriveS3Credentials({
  rootToken: "secret",
  bucket: "uploads",
});
```

```python
import boto3
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
    region_name="us-east-1",
)
s3.upload_file("photo.jpg", "uploads", "photos/cat.jpg")
```

Supported: PutObject, GetObject, HeadObject, DeleteObject, CopyObject, ListObjectsV2, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads, ListParts, CreateBucket, DeleteBucket, HeadBucket, ListBuckets.

## Authentication

The root token authenticates all buckets. Bucket-scoped tokens are derived with HMAC-SHA256 and require no database lookup — the server recomputes them on each request:

```ts
import { deriveToken } from "@beyond.dev/objects";

const uploadsBucketToken = deriveToken("secret", "uploads");
// Only valid for the "uploads" bucket — safe to share with clients
```

## Upload Tokens

Issue short-lived, key-scoped tokens for direct browser uploads without exposing the root or bucket token:

```ts
const { token, expiresAt } = await objects.createUploadToken("photos/cat.jpg", {
  ttlSecs: 300,
});
```

Pass the token to the browser. The browser uploads directly to the server:

```ts
import { createObjectsClient } from "@beyond.dev/objects";

const client = createObjectsClient({
  url: "http://localhost:9000",
  token: uploadToken,
  bucket: "uploads",
});
await client.put("photos/cat.jpg", file, { contentType: "image/jpeg" });
```

**React:**

```ts
import { useUpload } from "@beyond.dev/objects/react";

const { upload, progress, error } = useUpload({
  token: uploadToken,
  bucket: "uploads",
});
await upload("photos/cat.jpg", file);
```

## TypeScript SDK

**Next.js** — reads `BEYOND_OBJECTS_URL`, `BEYOND_OBJECTS_ROOT_TOKEN`, and `BEYOND_OBJECTS_BUCKET` from the environment:

```ts
import { objects } from "@beyond.dev/objects";

export async function uploadAction(data: FormData) {
  "use server";
  const file = data.get("file") as File;
  await objects.put(`uploads/${file.name}`, file, { contentType: file.type });
}
```

**Listing with pagination:**

```ts
let cursor: string | undefined;
do {
  const result = await objects.list({ prefix: "photos/", limit: 100, cursor });
  for (const obj of result.objects) { /* ... */ }
  cursor = result.nextCursor;
} while (cursor !== undefined);
```

**mTLS** (Node/Bun/Deno):

```ts
const objects = createObjectsClient({
  url: "https://objects.internal",
  token: "secret",
  tls: { ca: caPem, cert: certPem, key: keyPem },
});
```

## Buckets

Buckets are directories on disk. Manage them with the root token:

```sh
curl -X POST http://localhost:9000/v1/buckets \
  -H "Authorization: Bearer secret" \
  -d '{"name": "uploads"}'

curl -X PATCH http://localhost:9000/v1/buckets/uploads \
  -H "Authorization: Bearer secret" \
  -d '{"access": "public"}'
```

Or via the SDK:

```ts
await objects.buckets.create("uploads", { access: "private" });
await objects.buckets.update("uploads", { access: "public" });
const list = await objects.buckets.list();
await objects.buckets.delete("uploads");
```

## Configuration

| Env var                 | Default                 | Description                                                                       |
| ----------------------- | ----------------------- | --------------------------------------------------------------------------------- |
| `OBJECTS_ROOT_TOKEN`    | —                       | Root auth token; HMAC key for derived tokens (required)                           |
| `OBJECTS_DATA_DIR`      | `/data`                 | Root directory for buckets, temp files, and multipart state                       |
| `OBJECTS_INDEX_DIR`     | `/data/.index`          | LSM-tree index directory for prefix listing                                       |
| `ADDRESS`               | `0.0.0.0:9000`          | Bind address                                                                      |
| `OBJECTS_URL`           | —                       | Public base URL included in `url` fields on responses                             |
| `SYNC_LINGER_MS`        | `5`                     | fdatasync batching window — concurrent uploads within this window share one flush |
| `DRAIN_TIMEOUT_SECS`    | `30`                    | Grace period for in-flight requests during shutdown                               |
| `GC_TEMP_TTL_SECS`      | `3600`                  | Minimum age for orphan temp files eligible for garbage collection                 |
| `GC_MULTIPART_TTL_SECS` | `86400`                 | Minimum age for abandoned multipart uploads eligible for garbage collection       |
| `BEYOND_TLS_CERT`       | —                       | PEM-encoded TLS certificate                                                       |
| `BEYOND_TLS_KEY`        | —                       | PEM-encoded TLS private key                                                       |
| `BEYOND_TLS_CA`         | —                       | PEM-encoded CA cert; when set, mutual TLS is required on all connections          |
| `LOG_LEVEL`             | `info`                  | Log verbosity                                                                     |
| `OTLP_ENABLED`          | `false`                 | Export traces to an OTLP collector                                                |
| `OTLP_ENDPOINT`         | `http://localhost:4317` | OTLP collector gRPC address                                                       |
| `OTLP_SAMPLE_RATE`      | `0.1`                   | Fraction of traces sampled (0.0–1.0)                                              |

Set `ENVIRONMENT=development` for human-readable logs.

## Health

| Path       | Description                                              |
| ---------- | -------------------------------------------------------- |
| `/livez`   | Liveness — returns 200 when the process is up            |
| `/readyz`  | Readiness — returns 200 when the index is open and ready |
| `/metrics` | Prometheus metrics scrape endpoint                       |

## Development

```sh
mise run format   # format all source files
mise run test     # integration tests
mise run bench    # throughput benchmarks
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for on-disk layout, the atomic write path, HMAC derivation, S3 compatibility layer, and index design.

## License

MIT
