# @beyond.dev/objects

Store and retrieve objects on the Beyond platform — streaming uploads, conditional writes, prefix-paginated listing.

## Install

```sh
npm install @beyond.dev/objects
```

Requires Node.js 18+.

## Quick Start

```ts
import { createObjectsClient } from "@beyond.dev/objects";

// reads BEYOND_OBJECTS_URL + BEYOND_OBJECTS_ROOT_TOKEN from the environment
const objects = createObjectsClient();

const { data, error } = await objects.put("avatar.png", file, {
  contentType: "image/png",
  access: "public",
});
if (error) throw error;
console.log(data.url, data.etag);

const { data: stream } = await objects.get("avatar.png");
```

Every method returns a discriminated `{ data, error, response }` tuple — errors are values, not exceptions.

## API

### Client

```ts
createObjectsClient(opts?: ObjectsClientOptions): ObjectsClient
```

| Option       | Type       | Default                                 | Description                              |
| ------------ | ---------- | --------------------------------------- | ---------------------------------------- |
| `url`        | `string`   | `process.env.BEYOND_OBJECTS_URL`        | Base URL of the beyond-objects server    |
| `token`      | `string`   | `process.env.BEYOND_OBJECTS_ROOT_TOKEN` | Bearer token (root, or derived)          |
| `bucket`     | `string`   | `"default"`                             | Bucket this client operates on           |
| `fetch`      | `function` | `globalThis.fetch`                      | Custom fetch (for pooling or test mocks) |
| `timeout`    | `number`   | —                                       | Per-request timeout in milliseconds      |
| `retries`    | `number`   | `2`                                     | Max retries on transient 5xx failures    |
| `onRequest`  | `function` | —                                       | Called before each request               |
| `onResponse` | `function` | —                                       | Called after each response with duration |

### Objects

```ts
client.put(key, body, opts?):  ObjectsResult<PutResult>
client.get(key, opts?):        ObjectsResult<ReadableStream<Uint8Array>>
client.head(key):              ObjectsResult<HeadResult>
client.delete(key):            ObjectsResult               // 404 = success
client.move(from, to):         ObjectsResult<PutResult>
client.copy(from, to):         ObjectsResult<CopyResult>
client.setAccess(key, access): ObjectsResult<PutResult>
client.list(opts?):            ObjectsResult<ListResult>
client.url(key):               string                      // pure construction
```

`PutOptions`:

| Option        | Type                     | Description                                          |
| ------------- | ------------------------ | ---------------------------------------------------- |
| `contentType` | `string`                 | Stored alongside the object. Default `octet-stream`. |
| `access`      | `"public"\|"private"`    | Object visibility. Falls back to bucket default.     |
| `ifNoneMatch` | `"*"`                    | Write only when the object does not exist (CAS).     |
| `ifMatch`     | `string` (etag)          | Write only when the current etag matches (CAS).      |
| `metadata`    | `Record<string, string>` | User metadata, stored as `x-amz-meta-*` headers.     |

`GetOptions`:

| Option  | Type    | Description                                                   |
| ------- | ------- | ------------------------------------------------------------- |
| `range` | `Range` | Single byte range. Successful range responses are status 206. |

`Range` is `{ start: number; end?: number }` (inclusive end, omit for "to end of object") or `{ suffix: number }` (the trailing N bytes). Read `response.headers.get("content-range")` for the matched window.

`PutBody`: `string | Uint8Array | ArrayBuffer | Blob | ReadableStream<Uint8Array>`. Streams are forwarded directly — no buffering.

`HeadResult`: `{ size, etag, contentType, access, lastModified, metadata }`. `metadata` is a `Record<string, string>` with the `x-amz-meta-` prefix stripped from each header.

`ListOptions`:

| Option   | Type     | Description                                               |
| -------- | -------- | --------------------------------------------------------- |
| `prefix` | `string` | Only return keys with this prefix                         |
| `cursor` | `string` | Opaque cursor from `data.nextCursor` of the previous page |
| `limit`  | `number` | Max keys per page (default `1000`, max `1000`)            |

### Buckets

Bucket administration. Requires the root token; per-bucket derived tokens cannot manage buckets.

```ts
client.buckets.create(name, opts?): ObjectsResult<Bucket>     // idempotent
client.buckets.list():              ObjectsResult<Bucket[]>
client.buckets.get(name):           ObjectsResult<Bucket>
client.buckets.update(name, opts):  ObjectsResult<Bucket>
client.buckets.delete(name):        ObjectsResult            // 404 = success
```

### Tokens

```ts
deriveToken(rootToken: string, bucket: string): Promise<string>
```

Per-bucket tokens are `HMAC-SHA256(rootToken, bucketName)` in lowercase hex — derive locally and hand off to a downstream service. The server recomputes the same HMAC on every request; rotating the root token rotates all bucket tokens.

```ts
import { createObjectsClient, deriveToken } from "@beyond.dev/objects";

const imagesToken = await deriveToken(
  process.env.BEYOND_OBJECTS_ROOT_TOKEN!,
  "images",
);
const images = createObjectsClient({ bucket: "images", token: imagesToken });
```

```ts
createS3Credentials(rootToken: string, bucket: string): Promise<S3Credentials>
```

Derives AWS-style credentials (`accessKeyId`, `secretAccessKey`) for use with any S3-compatible client. Pass `"root"` to get root credentials.

```ts
import { S3Client } from "@aws-sdk/client-s3";
import { createS3Credentials } from "@beyond.dev/objects";

const creds = await createS3Credentials(
  process.env.BEYOND_OBJECTS_ROOT_TOKEN!,
  "images",
);
const s3 = new S3Client({
  endpoint: process.env.BEYOND_OBJECTS_URL,
  forcePathStyle: true,
  credentials: creds,
  region: "us-east-1",
});
```

## Examples

### Conditional writes (CAS)

```ts
// only write if the object does not exist
await objects.put("jobs/lock", payload, { ifNoneMatch: "*" });

// only write if the current etag matches
const { data } = await objects.head("config.json");
await objects.put("config.json", updated, { ifMatch: data.etag });
```

### Public objects

```ts
await objects.put("logo.svg", svg, {
  contentType: "image/svg+xml",
  access: "public",
});

// no token required to fetch
const res = await fetch(objects.url("logo.svg"));
```

### Streaming upload

```ts
import { createReadStream } from "node:fs";

const stream = ReadableStream.from(createReadStream("video.mp4"));
await objects.put("videos/clip.mp4", stream, { contentType: "video/mp4" });
```

### Range requests

```ts
const { data, response } = await objects.get("videos/clip.mp4", {
  range: { start: 0, end: 1023 },
});
console.log(response.status); // 206
console.log(response.headers.get("content-range")); // bytes 0-1023/<size>

// last 4 bytes
await objects.get("videos/clip.mp4", { range: { suffix: 4 } });
```

### User metadata

```ts
await objects.put("photo.jpg", file, {
  contentType: "image/jpeg",
  metadata: { owner: "u_123", traceId: "abc" },
});

const { data } = await objects.head("photo.jpg");
console.log(data.metadata); // { owner: "u_123", traceid: "abc" } — keys are lowercased
```

### Pagination

```ts
let cursor: string | undefined;
do {
  const { data } = await objects.list({ prefix: "logs/", cursor });
  if (!data) break;
  for (const o of data.objects) console.log(o.key);
  cursor = data.nextCursor;
} while (cursor);
```

### Observability

```ts
const objects = createObjectsClient({
  onRequest: (e) => logger.debug({ cmd: e.command }),
  onResponse: (e) =>
    metrics.histogram("objects.latency", e.durationMs, { cmd: e.command }),
});
```

### Error handling

```ts
import { ObjectsError } from "@beyond.dev/objects";

const { data, error } = await objects.get("missing.png");
if (error) {
  if (error.code === "object_not_found") {
    // 404
  } else {
    throw error;
  }
}
```

`error.code` is the stable contract — one of `unauthorized | forbidden | object_not_found | bucket_not_found | bucket_not_empty | object_exists | etag_mismatch | invalid_key | bad_request | range_not_satisfiable | internal_error`.

### Lifecycle

```ts
await objects.close(); // no-op for the HTTP transport, present for parity
```
