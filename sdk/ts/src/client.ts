import createFetchClient from "openapi-fetch";
import { ObjectsError } from "./errors.js";
import type { components, paths } from "./types.js";
import { type Camelize, camelize } from "./utils/camelize.js";

export { ObjectsError } from "./errors.js";
export type { components, operations, paths } from "./types.js";
export type { Camelize } from "./utils/camelize.js";

// ── Camelized API types (derived from the generated OpenAPI schema) ─────────

export type Bucket = Camelize<components["schemas"]["BucketResponse"]>;
export type ObjectItem = Camelize<components["schemas"]["ObjectItem"]>;
export type ListResult = Camelize<components["schemas"]["ListObjectsResponse"]>;
export type Access = "public" | "private";

// ── SDK option types ────────────────────────────────────────────────────────

export interface PutOptions {
  /** Stored alongside the object as `Content-Type`. Default `application/octet-stream`. */
  contentType?: string;
  /** Object visibility. Falls back to the bucket default when absent. */
  access?: Access;
  /** Set to `"*"` to write only when the object does not exist. */
  ifNoneMatch?: "*";
  /** Quoted etag — write only when the current etag matches. */
  ifMatch?: string;
  /**
   * User-defined metadata stored alongside the object and echoed back on
   * `head`/`get` as `x-amz-meta-{key}` headers. Keys are lowercased on the
   * wire; values must be valid HTTP header bytes (ASCII).
   */
  metadata?: Record<string, string>;
}

/**
 * A single-byte range request. `{ start, end? }` is `bytes=start-end` (inclusive
 * end; omitted means "to end of object"). `{ suffix }` is `bytes=-N` — the last
 * N bytes. Multi-range is intentionally unsupported (server returns 416).
 */
export type Range =
  | { start: number; end?: number; suffix?: never }
  | { suffix: number; start?: never; end?: never };

export interface GetOptions {
  /** Request a single byte range. Successful range responses come back as 206. */
  range?: Range;
}

export interface ListOptions {
  prefix?: string;
  cursor?: string;
  limit?: number;
}

export interface CreateBucketOptions {
  /** Default access level for objects in this bucket. Default `"private"`. */
  access?: Access;
}

export interface UpdateBucketOptions {
  access: Access;
}

export interface ObjectsRequestEvent {
  command: string;
}

export interface ObjectsResponseEvent {
  command: string;
  durationMs: number;
}

export interface ObjectsClientOptions {
  /** Base URL of the beyond-objects server. Defaults to `process.env.OBJECTS_URL`. */
  url?: string;
  /**
   * Bearer token. Use the root token for the default bucket and bucket admin,
   * or `deriveToken(rootToken, bucketName)` for a scoped token.
   * Defaults to `process.env.OBJECTS_ROOT_TOKEN`.
   */
  token?: string;
  /** Bucket this client operates against. Default `"default"`. */
  bucket?: string;
  /** Custom `fetch` implementation for test mocking or connection pooling. */
  fetch?: typeof globalThis.fetch;
  /** Per-request timeout in milliseconds. */
  timeout?: number;
  /** Max retry attempts on transient 5xx failures. Default: 2. */
  retries?: number;
  /** Called before each request. */
  onRequest?: (event: ObjectsRequestEvent) => void;
  /** Called after each response with the elapsed duration. */
  onResponse?: (event: ObjectsResponseEvent) => void;
}

export type ObjectsResult<T = undefined> = Promise<
  | { data: T; error: undefined; response: Response }
  | { data: undefined; error: ObjectsError; response: Response }
>;

export type PutBody =
  | string
  | Uint8Array
  | ArrayBuffer
  | Blob
  | ReadableStream<Uint8Array>;

export interface PutResult {
  /** Final key of the object. */
  key: string;
  /** Strong entity tag (quoted hex BLAKE3 of the object bytes). */
  etag: string;
  /** Object size in bytes. */
  size: number;
  /** Absolute URL where the object can be fetched. */
  url: string;
}

export interface CopyResult {
  key: string;
  etag: string;
  url: string;
}

export interface HeadResult {
  size: number;
  etag: string;
  contentType: string | undefined;
  access: Access;
  lastModified: Date | undefined;
  /** User metadata, with the `x-amz-meta-` prefix stripped. */
  metadata: Record<string, string>;
}

export interface ObjectsClient {
  /** Stream-upload an object. Honors `If-None-Match: "*"` and `If-Match: <etag>`. */
  put(
    key: string,
    body: PutBody,
    opts?: PutOptions,
  ): ObjectsResult<PutResult>;
  /**
   * Download an object as a byte stream. With `opts.range` set the response
   * is a 206 Partial Content; the `Content-Range` header is on `response.headers`.
   */
  get(
    key: string,
    opts?: GetOptions,
  ): ObjectsResult<ReadableStream<Uint8Array>>;
  /** Object metadata only (no body). */
  head(key: string): ObjectsResult<HeadResult>;
  /** Delete an object. 404s are treated as success (idempotent). */
  delete(key: string): ObjectsResult;
  /** Move (rename) an object within the same bucket. */
  move(from: string, to: string): ObjectsResult<PutResult>;
  /** Server-side copy within the same bucket. */
  copy(from: string, to: string): ObjectsResult<CopyResult>;
  /** Update an object's access level without moving it. */
  setAccess(key: string, access: Access): ObjectsResult<PutResult>;
  /** List object keys in this bucket. Prefix-scan with cursor pagination. */
  list(opts?: ListOptions): ObjectsResult<ListResult>;
  /** Bucket administration (root-token only). */
  buckets: BucketsClient;
  /** Build a stable absolute URL for a key. Pure construction, no I/O. */
  url(key: string): string;
  /** Release any underlying resources. No-op for the HTTP transport. */
  close(): Promise<void>;
}

export interface BucketsClient {
  /** Create a bucket. Idempotent — succeeds if it already exists. */
  create(name: string, opts?: CreateBucketOptions): ObjectsResult<Bucket>;
  /** List all buckets, sorted by name. */
  list(): ObjectsResult<Bucket[]>;
  /** Get bucket metadata. */
  get(name: string): ObjectsResult<Bucket>;
  /** Update bucket configuration. */
  update(name: string, opts: UpdateBucketOptions): ObjectsResult<Bucket>;
  /** Delete a bucket. 404s are treated as success. The bucket must be empty. */
  delete(name: string): ObjectsResult;
}

// ── Helpers (mirror Queue's client.ts byte-for-byte where shapes match) ─────

function toObjectsError(raw: unknown, response: Response): ObjectsError {
  const inner = raw != null && typeof raw === "object" && "error" in raw
    ? (raw as { error: { code?: string; message?: string; hint?: string } })
      .error
    : (raw as
      | { code?: string; message?: string; hint?: string }
      | undefined);
  const code = inner?.code ?? "internal_error";
  const message = inner?.message ?? "Unknown error";
  const hint = inner?.hint ?? undefined;
  return new ObjectsError(code, message, response.status, response, hint);
}

function wrap<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): ObjectsResult<Camelize<T>> {
  return promise.then(({ data, error, response }) =>
    error !== undefined
      ? {
        data: undefined,
        error: toObjectsError(error, response),
        response,
      }
      : {
        data: camelize(data) as Camelize<T>,
        error: undefined,
        response,
      }
  ) as unknown as ObjectsResult<Camelize<T>>;
}

function buildFetch(
  base: typeof globalThis.fetch | undefined,
  retries: number,
  timeout: number | undefined,
): typeof globalThis.fetch {
  const fetchFn = base ?? globalThis.fetch;
  return async (input, init) => {
    const signal = timeout != null
      ? AbortSignal.timeout(timeout)
      : init?.signal;
    const initWithSignal = signal != null ? { ...init, signal } : init;
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) {
        await new Promise<void>((r) => setTimeout(r, 100 * 2 ** (attempt - 1)));
      }
      let res: Response;
      try {
        res = await fetchFn(input, initWithSignal);
      } catch (err) {
        if (attempt >= retries) throw err;
        continue;
      }
      if (res.status >= 500 && attempt < retries) {
        await res.body?.cancel();
        continue;
      }
      return res;
    }
    throw new Error("unreachable");
  };
}

// Encode a key into a URL path. Slashes are preserved so `path/to/file.png`
// becomes a multi-segment URL — server handlers expect this.
function encodeKey(key: string): string {
  return key.split("/").map(encodeURIComponent).join("/");
}

const META_PREFIX = "x-amz-meta-";

function encodeRange(range: Range): string {
  if ("suffix" in range && range.suffix !== undefined) {
    return `bytes=-${range.suffix}`;
  }
  const end = range.end !== undefined ? String(range.end) : "";
  return `bytes=${range.start}-${end}`;
}

function readMetadata(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  headers.forEach((value, name) => {
    if (name.startsWith(META_PREFIX)) {
      out[name.slice(META_PREFIX.length)] = value;
    }
  });
  return out;
}

function readEnv(name: string): string | undefined {
  const proc = (globalThis as unknown as {
    process?: { env?: Record<string, string | undefined> };
  }).process;
  return proc?.env?.[name];
}

// ── Factory ─────────────────────────────────────────────────────────────────

/** Create an Objects client scoped to a single bucket. */
export function createObjectsClient(
  opts: ObjectsClientOptions = {},
): ObjectsClient {
  const url = opts.url ?? readEnv("OBJECTS_URL");
  if (url == null || url === "") {
    throw new ObjectsError(
      "invalid_request",
      "OBJECTS_URL is required (pass `url` or set the OBJECTS_URL env var)",
      0,
    );
  }
  const token = opts.token ?? readEnv("OBJECTS_ROOT_TOKEN");
  if (token == null || token === "") {
    throw new ObjectsError(
      "invalid_request",
      "Bearer token is required (pass `token` or set the OBJECTS_ROOT_TOKEN env var)",
      0,
    );
  }

  const base = url.replace(/\/+$/, "");
  const bucket = opts.bucket ?? "default";
  const authHeader = `Bearer ${token}`;
  const { onRequest, onResponse } = opts;

  const fetchFn = buildFetch(opts.fetch, opts.retries ?? 2, opts.timeout);

  const client = createFetchClient<paths>({
    baseUrl: base,
    headers: { Authorization: authHeader },
    fetch: fetchFn,
  });

  // Wraps a method to fire onRequest/onResponse hooks around it.
  function cmd<A extends unknown[], R>(
    name: string,
    fn: (...args: A) => Promise<R>,
  ): (...args: A) => Promise<R> {
    return async (...args) => {
      onRequest?.({ command: name });
      const start = Date.now();
      try {
        return await fn(...args);
      } finally {
        onResponse?.({ command: name, durationMs: Date.now() - start });
      }
    };
  }

  function objectUrl(key: string): string {
    return `${base}/v1/${encodeURIComponent(bucket)}/${encodeKey(key)}`;
  }

  // ── Object operations (raw fetch for binary / streaming bodies) ────────────

  const put: ObjectsClient["put"] = cmd(
    "put",
    async (key, body, putOpts) => {
      const headers: Record<string, string> = {
        Authorization: authHeader,
        "Content-Type": putOpts?.contentType ?? "application/octet-stream",
      };
      if (putOpts?.ifNoneMatch != null) {
        headers["If-None-Match"] = putOpts.ifNoneMatch;
      }
      if (putOpts?.ifMatch != null) headers["If-Match"] = putOpts.ifMatch;
      if (putOpts?.access != null) headers["X-Beyond-Access"] = putOpts.access;
      if (putOpts?.metadata != null) {
        for (const [k, v] of Object.entries(putOpts.metadata)) {
          headers[`${META_PREFIX}${k.toLowerCase()}`] = v;
        }
      }

      const init: RequestInit = {
        method: "PUT",
        headers,
        body: body as BodyInit,
      };
      if (body instanceof ReadableStream) {
        // Node fetch requires `duplex: "half"` for streaming request bodies.
        (init as RequestInit & { duplex: "half" }).duplex = "half";
      }

      const response = await fetchFn(objectUrl(key), init);
      if (!response.ok) {
        let parsed: unknown;
        try {
          parsed = await response.json();
        } catch {
          /* fall through with empty body */
        }
        return {
          data: undefined,
          error: toObjectsError(parsed, response),
          response,
        };
      }
      const raw =
        (await response.json()) as components["schemas"]["PutObjectResponse"];
      return {
        data: {
          key: raw.key,
          etag: raw.etag,
          size: raw.size,
          url: objectUrl(raw.key),
        },
        error: undefined,
        response,
      };
    },
  );

  const get: ObjectsClient["get"] = cmd("get", async (key, getOpts) => {
    const reqHeaders: Record<string, string> = { Authorization: authHeader };
    if (getOpts?.range != null) {
      reqHeaders["Range"] = encodeRange(getOpts.range);
    }
    const response = await fetchFn(objectUrl(key), {
      method: "GET",
      headers: reqHeaders,
    });
    if (!response.ok) {
      let parsed: unknown;
      try {
        parsed = await response.json();
      } catch {
        /* fall through */
      }
      return {
        data: undefined,
        error: toObjectsError(parsed, response),
        response,
      };
    }
    const stream = response.body;
    if (stream == null) {
      return {
        data: undefined,
        error: new ObjectsError(
          "internal_error",
          "response had no body",
          response.status,
          response,
        ),
        response,
      };
    }
    return { data: stream, error: undefined, response };
  });

  const head: ObjectsClient["head"] = cmd("head", async (key) => {
    const response = await fetchFn(objectUrl(key), {
      method: "HEAD",
      headers: { Authorization: authHeader },
    });
    if (!response.ok) {
      // HEAD has no body; build a stub error from status alone.
      return {
        data: undefined,
        error: toObjectsError(
          {
            code: response.status === 404 ? "object_not_found" : "unauthorized",
          },
          response,
        ),
        response,
      };
    }
    const sizeHeader = response.headers.get("content-length");
    const etag = response.headers.get("etag") ?? "";
    const contentType = response.headers.get("content-type") ?? undefined;
    const lastModifiedRaw = response.headers.get("last-modified");
    const cors = response.headers.get("access-control-allow-origin");
    const lastModified = lastModifiedRaw != null
      ? new Date(lastModifiedRaw)
      : undefined;
    return {
      data: {
        size: sizeHeader != null ? Number(sizeHeader) : 0,
        etag,
        contentType,
        access: cors === "*" ? "public" : "private",
        lastModified,
        metadata: readMetadata(response.headers),
      },
      error: undefined,
      response,
    };
  });

  const del: ObjectsClient["delete"] = cmd("delete", async (key) => {
    const { error, response } = await client.DELETE("/v1/{bucket}/{key}", {
      params: { path: { bucket, key } },
    });
    if (error && response.status !== 404) {
      return {
        data: undefined,
        error: toObjectsError(error, response),
        response,
      };
    }
    return { data: undefined, error: undefined, response };
  });

  async function patchObject(
    key: string,
    body: components["schemas"]["PatchObjectRequest"],
  ): ReturnType<ObjectsClient["move"]> {
    const { data, error, response } = await client.PATCH(
      "/v1/{bucket}/{key}",
      { params: { path: { bucket, key } }, body },
    );
    if (error) {
      return {
        data: undefined,
        error: toObjectsError(error, response),
        response,
      };
    }
    const raw = data as components["schemas"]["PutObjectResponse"];
    return {
      data: {
        key: raw.key,
        etag: raw.etag,
        size: raw.size,
        url: objectUrl(raw.key),
      },
      error: undefined,
      response,
    };
  }

  const move: ObjectsClient["move"] = cmd(
    "move",
    (from, to) => patchObject(from, { key: to }),
  );

  const setAccess: ObjectsClient["setAccess"] = cmd(
    "setAccess",
    (key, access) => patchObject(key, { access }),
  );

  const copy: ObjectsClient["copy"] = cmd("copy", async (from, to) => {
    const { data, error, response } = await client.POST("/v1/{bucket}/{key}", {
      params: { path: { bucket, key: to } },
      body: { source: from },
    });
    if (error) {
      return {
        data: undefined,
        error: toObjectsError(error, response),
        response,
      };
    }
    const raw = data as components["schemas"]["CopyObjectResponse"];
    return {
      data: { key: raw.key, etag: raw.etag, url: objectUrl(raw.key) },
      error: undefined,
      response,
    };
  });

  const list: ObjectsClient["list"] = cmd("list", (listOpts) =>
    wrap(
      client.GET("/v1/{bucket}", {
        params: {
          path: { bucket },
          query: {
            ...(listOpts?.prefix !== undefined && { prefix: listOpts.prefix }),
            ...(listOpts?.cursor !== undefined && { cursor: listOpts.cursor }),
            ...(listOpts?.limit !== undefined && { limit: listOpts.limit }),
          },
        },
      }),
    ));

  // ── Bucket admin ───────────────────────────────────────────────────────────

  const buckets: BucketsClient = {
    create: cmd("buckets.create", (name, bOpts) =>
      wrap(
        client.POST("/v1/buckets", {
          body: {
            name,
            ...(bOpts?.access !== undefined && { access: bOpts.access }),
          },
        }),
      )),

    list: cmd("buckets.list", async () => {
      const { data, error, response } = await client.GET("/v1/buckets", {});
      if (error) {
        return {
          data: undefined,
          error: toObjectsError(error, response),
          response,
        };
      }
      const raw = data as components["schemas"]["ListBucketsResponse"];
      return {
        data: camelize(raw.buckets) as Bucket[],
        error: undefined,
        response,
      };
    }),

    get: cmd("buckets.get", (name) =>
      wrap(
        client.GET("/v1/buckets/{name}", { params: { path: { name } } }),
      )),

    update: cmd("buckets.update", (name, bOpts) =>
      wrap(
        client.PATCH("/v1/buckets/{name}", {
          params: { path: { name } },
          body: { access: bOpts.access },
        }),
      )),

    delete: cmd("buckets.delete", async (name) => {
      const { error, response } = await client.DELETE("/v1/buckets/{name}", {
        params: { path: { name } },
      });
      if (error && response.status !== 404) {
        return {
          data: undefined,
          error: toObjectsError(error, response),
          response,
        };
      }
      return { data: undefined, error: undefined, response };
    }),
  };

  return {
    put,
    get,
    head,
    delete: del,
    move,
    copy,
    setAccess,
    list,
    buckets,
    url: objectUrl,
    close: () => Promise.resolve(),
  };
}

// ── Token derivation (mirrors server's HMAC-SHA256 in lowercase hex) ────────

/**
 * Derive a per-bucket bearer token from the root token.
 *
 * Server-side validation: `HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket_name)` in
 * lowercase hex. Implemented with Web Crypto so it works in Node, browsers,
 * and edge runtimes.
 */
export async function deriveToken(
  rootToken: string,
  bucket: string,
): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(rootToken),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(bucket));
  return [...new Uint8Array(sig)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}
