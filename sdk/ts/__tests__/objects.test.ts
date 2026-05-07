import { createHash, randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { ObjectsError } from "../src/index.js";
import {
  bucketClient,
  dec,
  enc,
  readAll,
  rootClient,
  uniqueBucket,
  uniqueKey,
} from "./harness.js";

describe("objects — put/get/head/delete", () => {
  it("round-trips a string body", async () => {
    const c = rootClient();
    const key = uniqueKey();
    const { data: putData, error: putErr } = await c.put(key, "hello world");
    expect(putErr).toBeUndefined();
    expect(putData?.key).toBe(key);
    expect(putData?.size).toBe(11);
    expect(putData?.url).toMatch(/\/v1\/default\//);

    const { data: stream, error: getErr } = await c.get(key);
    expect(getErr).toBeUndefined();
    expect(stream).toBeDefined();
    const bytes = await readAll(stream!);
    expect(dec(bytes)).toBe("hello world");
  });

  it("round-trips binary bytes", async () => {
    const c = rootClient();
    const key = uniqueKey();
    const payload = new Uint8Array([0, 1, 2, 3, 4, 254, 255]);
    const { data, error } = await c.put(key, payload, {
      contentType: "application/octet-stream",
    });
    expect(error).toBeUndefined();
    expect(data?.size).toBe(payload.byteLength);

    const { data: stream } = await c.get(key);
    const round = await readAll(stream!);
    expect(round).toEqual(payload);
  });

  it("HEAD returns size, etag, content-type, last-modified, access, metadata", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "metadata-test", { contentType: "text/plain" });

    const { data, error } = await c.head(key);
    expect(error).toBeUndefined();
    expect(data?.size).toBe(13);
    expect(data?.etag).toMatch(/^"[0-9a-f]+"$/);
    expect(data?.contentType).toBe("text/plain");
    expect(data?.access).toBe("private");
    expect(data?.lastModified).toBeInstanceOf(Date);
    expect(data?.metadata).toEqual({});
  });

  it("delete is idempotent — 404 returns no error", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "x");
    const first = await c.delete(key);
    expect(first.error).toBeUndefined();
    const second = await c.delete(key);
    expect(second.error).toBeUndefined();
  });

  it("get on a missing object returns object_not_found", async () => {
    const c = rootClient();
    const { error, response } = await c.get(uniqueKey());
    expect(response.status).toBe(404);
    expect(error).toBeInstanceOf(ObjectsError);
    expect(error?.code).toBe("object_not_found");
    // The error carries the raw Response so callers can still inspect headers
    // (Retry-After, WWW-Authenticate, etc.) after a rethrow.
    expect(error?.response).toBe(response);
    expect(error?.response?.status).toBe(404);
  });

  it("multi-segment keys are preserved in the url", async () => {
    const c = rootClient();
    const key = `nested/path/${uniqueKey()}.txt`;
    const { data } = await c.put(key, "deep");
    expect(data?.key).toBe(key);
    expect(data?.url).toContain(`/${encodeURIComponent("nested")}/`);
  });

  it("public objects can be read without auth", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "public-blob", { access: "public" });

    const url = c.url(key);
    const res = await fetch(url);
    expect(res.status).toBe(200);
    expect(res.headers.get("access-control-allow-origin")).toBe("*");
    expect(await res.text()).toBe("public-blob");
  });

  it("public/private flips via setAccess", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "x", { access: "private" });
    const noAuth = await fetch(c.url(key));
    expect(noAuth.status).toBe(401);
    await noAuth.body?.cancel();

    const flip = await c.setAccess(key, "public");
    expect(flip.error).toBeUndefined();
    const ok = await fetch(c.url(key));
    expect(ok.status).toBe(200);
    await ok.body?.cancel();
  });

  it("uploads a stream body", async () => {
    const c = rootClient();
    const key = uniqueKey();
    const payload = enc("streamed-content");
    const stream = new ReadableStream<Uint8Array>({
      start(ctrl) {
        ctrl.enqueue(payload);
        ctrl.close();
      },
    });
    const { data, error } = await c.put(key, stream);
    expect(error).toBeUndefined();
    expect(data?.size).toBe(payload.byteLength);

    const { data: out } = await c.get(key);
    expect(dec(await readAll(out!))).toBe("streamed-content");
  });

  // Proof that bytes flow end-to-end without being buffered into a single
  // in-memory blob: push 4 MiB through PUT as a multi-chunk stream and assert
  // GET delivers more than one chunk to the consumer.
  it("streams multi-chunk payloads without buffering in either direction", async () => {
    const c = rootClient();
    const key = uniqueKey();

    const chunkSize = 256 * 1024;
    const chunkCount = 16;
    const chunks: Uint8Array[] = Array.from(
      { length: chunkCount },
      () => randomBytes(chunkSize),
    );
    const total = chunkSize * chunkCount;
    const expectedHash = createHash("sha256");
    for (const c of chunks) expectedHash.update(c);
    const expected = expectedHash.digest("hex");

    let emitted = 0;
    const upload = new ReadableStream<Uint8Array>({
      pull(ctrl) {
        if (emitted >= chunks.length) {
          ctrl.close();
          return;
        }
        ctrl.enqueue(chunks[emitted]!);
        emitted++;
      },
    });

    const put = await c.put(key, upload, {
      contentType: "application/octet-stream",
    });
    expect(put.error).toBeUndefined();
    expect(put.data?.size).toBe(total);
    expect(emitted).toBe(chunkCount);

    const got = await c.get(key);
    expect(got.error).toBeUndefined();
    const reader = got.data!.getReader();
    const observed = createHash("sha256");
    let observedTotal = 0;
    let reads = 0;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value != null) {
        reads++;
        observedTotal += value.byteLength;
        observed.update(value);
      }
    }
    expect(observedTotal).toBe(total);
    expect(observed.digest("hex")).toBe(expected);
    // > 1 read() call proves the SDK hands back a live stream rather than a
    // single buffered blob; the kernel can split anywhere, so we just assert
    // the consumer saw progressive chunks.
    expect(reads).toBeGreaterThan(1);
  });

  it("scoped bucket client can put/get against its bucket", async () => {
    const bucket = uniqueBucket();
    const c = await bucketClient(bucket);
    const key = uniqueKey();
    await c.put(key, "scoped");
    const { data } = await c.get(key);
    expect(dec(await readAll(data!))).toBe("scoped");
  });
});

describe("objects — move and copy", () => {
  it("move renames within the same bucket", async () => {
    const c = rootClient();
    const from = uniqueKey("from");
    const to = uniqueKey("to");
    await c.put(from, "movable");
    const { data, error } = await c.move(from, to);
    expect(error).toBeUndefined();
    expect(data?.key).toBe(to);

    const gone = await c.head(from);
    expect(gone.response.status).toBe(404);

    const { data: stream } = await c.get(to);
    expect(dec(await readAll(stream!))).toBe("movable");
  });

  it("copy duplicates within the same bucket", async () => {
    const c = rootClient();
    const src = uniqueKey("src");
    const dst = uniqueKey("dst");
    await c.put(src, "copyable");
    const { data, error } = await c.copy(src, dst);
    expect(error).toBeUndefined();
    expect(data?.key).toBe(dst);

    const a = await c.get(src);
    const b = await c.get(dst);
    expect(dec(await readAll(a.data!))).toBe("copyable");
    expect(dec(await readAll(b.data!))).toBe("copyable");
  });
});
