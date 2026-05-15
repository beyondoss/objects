import createFetchClient from "openapi-fetch";
import { env } from "std-env";
import { ObjectsError } from "./errors.js";
import type { components, paths } from "./types.js";
import { type Camelize, camelize } from "./utils/camelize.js";

export { ObjectsError } from "./errors.js";
export type { components, operations, paths } from "./types.js";
export type { Camelize } from "./utils/camelize.js";

// ── Camelized API types (derived from the generated OpenAPI schema) ─────────

export type Bucket = Camelize<components["schemas"]["BucketResponse"]>;
export type ObjectItem = Camelize<components["schemas"]["ObjectItem"]>;
export interface ListResult {
  objects: ObjectItem[];
  /** Opaque cursor for the next page; `undefined` when the page is final. */
  nextCursor: string | undefined;
}
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
  /**
   * Opaque cursor from `data.nextCursor` of the previous page. Explicitly
   * accepts `undefined` so the natural pagination idiom — `cursor =
   * data.nextCursor` — type-checks under `exactOptionalPropertyTypes`.
   */
  cursor?: string | undefined;
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

/**
 * mTLS / custom-CA options. Supply all three fields for mutual TLS, or just
 * `ca` to pin a private CA without presenting a client certificate.
 *
 * - **Node / Bun**: forwarded to an undici `Agent` with `allowH2: true` so you
 *   still get HTTP/2 multiplexing over the TLS connection.
 * - **Deno**: forwarded to `Deno.createHttpClient`.
 * - **Browser / edge runtimes**: silently ignored — the platform owns TLS.
 */
export interface TlsOptions {
  /** PEM-encoded CA certificate (or array of certificates) to trust. */
  ca?: string | string[];
  /** PEM-encoded client certificate to present during the TLS handshake. */
  cert?: string;
  /** PEM-encoded private key matching `cert`. */
  key?: string;
}

export interface ObjectsClientOptions {
  /** Base URL of the beyond-objects server. Defaults to the `BEYOND_OBJECTS_URL` env var. */
  url?: string;
  /**
   * Bearer token. Use the root token for the default bucket and bucket admin,
   * or `deriveToken(rootToken, bucketName)` for a scoped token.
   * Defaults to the `BEYOND_OBJECTS_ROOT_TOKEN` env var.
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
  /**
   * TLS / mTLS options. When provided, the SDK builds a TLS-aware fetch
   * instead of the plain H2 fetch. See {@link TlsOptions} for per-runtime
   * behaviour.
   */
  tls?: TlsOptions;
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

export interface UploadTokenResult {
  /** Short-lived Bearer token to present when calling `PUT /v1/{bucket}/{key}`. */
  token: string;
  /** Unix timestamp (seconds) after which the token is rejected. */
  expiresAt: number;
}

export interface ObjectsClient {
  /**
   * Issue a short-lived upload token scoped to a single object key. Pass this
   * token to a browser client so it can PUT directly to the objects server
   * without holding a long-lived credential.
   *
   * @param key - The exact object key the browser will upload to.
   * @param opts.ttlSecs - Token lifetime in seconds (1–86400). Default 3600.
   */
  createUploadToken(
    key: string,
    opts?: { ttlSecs?: number },
  ): ObjectsResult<UploadTokenResult>;
  /** Stream-upload an object. Honors `If-None-Match: "*"` and `If-Match: <etag>`. */
  put(key: string, body: PutBody, opts?: PutOptions): ObjectsResult<PutResult>;
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

// ── h2c default fetch ────────────────────────────────────────────────────────
// In Node 18+ (our minimum), undici is a built-in. Using an Agent with
// allowH2: true makes every cleartext HTTP connection automatically use HTTP/2
// when the server supports it — no TLS, no user config needed. In browsers and
// edge runtimes the import fails silently and globalThis.fetch is used instead
// (which already negotiates h2 via ALPN for https endpoints).

let _h2Fetch: typeof globalThis.fetch | undefined;
// Use a variable so bundlers don't statically resolve the import.
const _undici = "undici";
const _h2FetchInit: Promise<void> = (import(_undici) as Promise<any>)
  .then(({ fetch: f, Agent }: any) => {
    const agent = new Agent({ allowH2: true });
    _h2Fetch = (url, init) =>
      f(url, { ...(init ?? {}), dispatcher: agent }) as Promise<Response>;
  })
  .catch(() => {
    /* not Node or undici unavailable — fall back to globalThis.fetch */
  });

// ── TLS-aware fetch builder ──────────────────────────────────────────────────

/**
 * Build a TLS-aware fetch function for the given {@link TlsOptions}.
 * Returns a Promise so the undici import can be awaited once and reused.
 *
 * - Deno: uses `Deno.createHttpClient` (native TLS support).
 * - Node / Bun: creates an undici `Agent` with `allowH2: true` + `connect`
 *   options so you get mTLS *and* HTTP/2 multiplexing.
 * - Browser / edge: returns `globalThis.fetch` unchanged (platform owns TLS).
 */
function buildTlsFetchPromise(
  tls: TlsOptions,
): Promise<typeof globalThis.fetch> {
  const cas = Array.isArray(tls.ca) ? tls.ca : tls.ca ? [tls.ca] : undefined;

  // Deno
  const g = globalThis as any;
  if (
    typeof g.Deno !== "undefined" &&
    typeof g.Deno.createHttpClient === "function"
  ) {
    const client = g.Deno.createHttpClient({
      caCerts: cas,
      certChain: tls.cert,
      privateKey: tls.key,
    });
    return Promise.resolve(
      (url: RequestInfo | URL, init?: RequestInit) =>
        globalThis.fetch(url, { ...init, client } as any),
    );
  }

  // Node / Bun — try undici Agent with allowH2 + TLS connect options first
  // (best: HTTP/2 + mTLS), then fall back to a node:https based fetch for
  // environments where undici isn't available as a standalone package.
  return (import(_undici) as Promise<any>)
    .then(({ fetch: f, Agent }: any) => {
      const connect: Record<string, unknown> = {};
      if (cas != null) connect["ca"] = cas;
      if (tls.cert != null) connect["cert"] = tls.cert;
      if (tls.key != null) connect["key"] = tls.key;
      const agent = new Agent({ allowH2: true, connect });
      return (url: RequestInfo | URL, init?: RequestInit) => {
        if (url instanceof Request) {
          const req = url as Request;
          return f(req.url, {
            method: req.method,
            headers: req.headers,
            body: req.body,
            ...(init ?? {}),
            dispatcher: agent,
          }) as Promise<Response>;
        }
        return f(url, { ...(init ?? {}), dispatcher: agent }) as Promise<Response>;
      };
    })
    .catch(() =>
      // undici unavailable — fall back to node:https with TLS options.
      // This gives HTTP/1.1 with full mTLS; browsers/edge never reach here.
      (import("node:https") as Promise<any>)
        .then(({ request }: any) => {
          return (
            url: RequestInfo | URL,
            init?: RequestInit,
          ): Promise<Response> => {
            // openapi-fetch passes a Request object as the first arg; extract
            // url, method, headers, and body from it, then let init override.
            const isRequest = typeof url === "object" && url instanceof Request;
            const href = isRequest
              ? (url as Request).url
              : url instanceof URL
                ? url.href
                : (url as string);
            const parsed = new URL(href);
            const method = (
              init?.method ??
              (isRequest ? (url as Request).method : "GET")
            ).toUpperCase();

            // Merge headers: Request headers first, then init.headers on top
            const headersRecord: Record<string, string> = {};
            if (isRequest) {
              (url as Request).headers.forEach(
                (v: string, k: string) => { headersRecord[k] = v; },
              );
            }
            const initHeaders = init?.headers;
            if (initHeaders != null) {
              if (initHeaders instanceof Headers) {
                initHeaders.forEach((v, k) => { headersRecord[k] = v; });
              } else if (Array.isArray(initHeaders)) {
                for (const [k, v] of initHeaders as [string, string][]) {
                  headersRecord[k] = v;
                }
              } else {
                Object.assign(
                  headersRecord,
                  initHeaders as Record<string, string>,
                );
              }
            }

            const tlsOpts: Record<string, unknown> = {
              rejectUnauthorized: true,
            };
            if (cas != null) tlsOpts["ca"] = cas;
            if (tls.cert != null) tlsOpts["cert"] = tls.cert;
            if (tls.key != null) tlsOpts["key"] = tls.key;

            // Determine body: init.body wins, then Request body
            const rawBody = init?.body ??
              (isRequest ? (url as Request).body : null);

            return new Promise((resolve, reject) => {
              const options = {
                hostname: parsed.hostname,
                port: parsed.port || 443,
                path: parsed.pathname + parsed.search,
                method,
                headers: headersRecord,
                ...tlsOpts,
              };
              const req = request(options, (res: any) => {
                const chunks: Buffer[] = [];
                res.on("data", (c: Buffer) => chunks.push(c));
                res.on("end", () => {
                  const body = Buffer.concat(chunks);
                  const headers = new Headers();
                  for (const [k, v] of Object.entries(
                    res.headers as Record<string, string | string[]>,
                  )) {
                    const vals = Array.isArray(v) ? v : [v];
                    for (const val of vals) headers.append(k, val);
                  }
                  resolve(
                    new Response(body, {
                      status: res.statusCode ?? 200,
                      headers,
                    }),
                  );
                });
                res.on("error", reject);
              });
              req.on("error", reject);
              if (rawBody != null) {
                if (typeof (rawBody as any).pipe === "function") {
                  (rawBody as any).pipe(req);
                } else if (rawBody instanceof Uint8Array) {
                  req.write(rawBody);
                  req.end();
                } else if (typeof rawBody === "string") {
                  req.write(rawBody);
                  req.end();
                } else {
                  req.end();
                }
              } else {
                req.end();
              }
            });
          };
        })
        .catch(() => globalThis.fetch)
    );
}

// ── Helpers (mirror Queue's client.ts byte-for-byte where shapes match) ─────

function toObjectsError(raw: unknown, response: Response): ObjectsError {
  const inner = raw != null && typeof raw === "object" && "error" in raw
    ? (raw as { error: { code?: string; message?: string; hint?: string } })
      .error
    : (raw as { code?: string; message?: string; hint?: string } | undefined);
  const code = inner?.code ?? "internal_error";
  const message = inner?.message ?? "Unknown error";
  const hint = inner?.hint ?? undefined;
  return new ObjectsError(code, message, response.status, response, hint);
}

function wrap<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): ObjectsResult<Camelize<T>> {
  return promise.then(
    ({
      data,
      error,
      response,
    }):
      | { data: Camelize<T>; error: undefined; response: Response }
      | { data: undefined; error: ObjectsError; response: Response } =>
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
        },
  );
}

function buildFetch(
  base: typeof globalThis.fetch | undefined,
  tlsFetchPromise: Promise<typeof globalThis.fetch> | undefined,
  retries: number,
  timeout: number | undefined,
): typeof globalThis.fetch {
  let resolvedTls: typeof globalThis.fetch | undefined;
  const tlsInit = tlsFetchPromise?.then((f) => {
    resolvedTls = f;
  });

  return async (input, init) => {
    if (tlsInit) await tlsInit;
    if (!resolvedTls && !_h2Fetch) await _h2FetchInit;
    const fetchFn = base ?? resolvedTls ?? _h2Fetch ?? globalThis.fetch;
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

// ── Factory ─────────────────────────────────────────────────────────────────

/** Create an Objects client scoped to a single bucket. */
export function createObjectsClient(
  opts: ObjectsClientOptions = {},
): ObjectsClient {
  const url = opts.url ?? env["BEYOND_OBJECTS_URL"];
  if (url == null || url === "") {
    throw new ObjectsError(
      "invalid_request",
      "BEYOND_OBJECTS_URL is required (pass `url` or set the BEYOND_OBJECTS_URL env var)",
      0,
    );
  }
  const token = opts.token ?? env["BEYOND_OBJECTS_ROOT_TOKEN"];
  if (token == null || token === "") {
    throw new ObjectsError(
      "invalid_request",
      "Bearer token is required (pass `token` or set the BEYOND_OBJECTS_ROOT_TOKEN env var)",
      0,
    );
  }

  const base = url.replace(/\/+$/, "");
  const bucket = opts.bucket ?? "default";
  const authHeader = `Bearer ${token}`;
  const { onRequest, onResponse } = opts;

  const fetchFn = buildFetch(
    opts.fetch,
    opts.tls ? buildTlsFetchPromise(opts.tls) : undefined,
    opts.retries ?? 2,
    opts.timeout,
  );

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

  const put: ObjectsClient["put"] = cmd("put", async (key, body, putOpts) => {
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
      // BodyInit narrows ArrayBuffer to exclude SharedArrayBuffer; Uint8Array
      // backed by ArrayBufferLike is rejected by lib.dom even though fetch
      // accepts it at runtime in every supported runtime.
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
  });

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
    const accessHeader = response.headers.get("x-beyond-access");
    const lastModified = lastModifiedRaw != null
      ? new Date(lastModifiedRaw)
      : undefined;
    return {
      data: {
        size: sizeHeader != null ? Number(sizeHeader) : 0,
        etag,
        contentType,
        access: accessHeader === "public" ? "public" : "private",
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
    const { data, error, response } = await client.PATCH("/v1/{bucket}/{key}", {
      params: { path: { bucket, key } },
      body,
    });
    if (error) {
      return {
        data: undefined,
        error: toObjectsError(error, response),
        response,
      };
    }
    return {
      data: {
        key: data.key,
        etag: data.etag,
        size: data.size,
        url: objectUrl(data.key),
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
    return {
      data: { key: data.key, etag: data.etag, url: objectUrl(data.key) },
      error: undefined,
      response,
    };
  });

  const list: ObjectsClient["list"] = cmd("list", async (listOpts) => {
    const { data, error, response } = await client.GET("/v1/{bucket}", {
      params: {
        path: { bucket },
        query: {
          ...(listOpts?.prefix !== undefined && { prefix: listOpts.prefix }),
          ...(listOpts?.cursor !== undefined && { cursor: listOpts.cursor }),
          ...(listOpts?.limit !== undefined && { limit: listOpts.limit }),
        },
      },
    });
    if (error) {
      return {
        data: undefined,
        error: toObjectsError(error, response),
        response,
      };
    }
    return {
      data: {
        objects: camelize(data.objects),
        nextCursor: data.next_cursor ?? undefined,
      },
      error: undefined,
      response,
    };
  });

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
      return {
        data: camelize(data.buckets),
        error: undefined,
        response,
      };
    }),

    get: cmd(
      "buckets.get",
      (name) =>
        wrap(client.GET("/v1/buckets/{name}", { params: { path: { name } } })),
    ),

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

  const createUploadToken: ObjectsClient["createUploadToken"] = cmd(
    "createUploadToken",
    async (key, tokenOpts) => {
      const response = await fetchFn(
        `${base}/v1/${encodeURIComponent(bucket)}/upload-tokens`,
        {
          method: "POST",
          headers: {
            Authorization: authHeader,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            key,
            ...(tokenOpts?.ttlSecs !== undefined && {
              ttl_secs: tokenOpts.ttlSecs,
            }),
          }),
        },
      );
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
      const raw = (await response.json()) as {
        token: string;
        expires_at: number;
      };
      return {
        data: { token: raw.token, expiresAt: raw.expires_at },
        error: undefined,
        response,
      };
    },
  );

  return {
    createUploadToken,
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
 * Server-side validation: `HMAC-SHA256(BEYOND_OBJECTS_ROOT_TOKEN, bucket_name)` in
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

/**
 * AWS-style credentials for the S3-compatible surface. Pass these to any
 * `aws-sdk-*` / S3 client alongside the service URL and `forcePathStyle:
 * true`.
 *
 * The mapping is the inverse of `deriveToken`: the access key id is the
 * bucket name (or the literal `"root"` for the root scope), and the secret
 * is the same string a REST client puts in `Authorization: Bearer …`.
 */
export interface S3Credentials {
  accessKeyId: string;
  secretAccessKey: string;
}

/**
 * Derive AWS-style S3 credentials. `bucket = "root"` returns the root
 * credentials; any other bucket name returns scoped credentials.
 *
 * @example
 * ```ts
 * const creds = await deriveS3Credentials(process.env.BEYOND_OBJECTS_ROOT_TOKEN, "images");
 * const s3 = new S3Client({
 *   endpoint: process.env.BEYOND_OBJECTS_URL,
 *   forcePathStyle: true,
 *   credentials: creds,
 *   region: "us-east-1",
 * });
 * ```
 */
export async function createS3Credentials(
  rootToken: string,
  bucket: string,
): Promise<S3Credentials> {
  if (bucket === "root") {
    return { accessKeyId: "root", secretAccessKey: rootToken };
  }
  return {
    accessKeyId: bucket,
    secretAccessKey: await deriveToken(rootToken, bucket),
  };
}
