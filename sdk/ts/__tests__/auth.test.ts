import { createHmac } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createObjectsClient, deriveToken } from "../src/index.js";
import {
  ensureBucket,
  getRootToken,
  getTestUrl,
  rootClient,
  uniqueBucket,
  uniqueKey,
} from "./harness.js";

describe("auth — root and derived tokens", () => {
  it("root token works for the default bucket", async () => {
    const c = rootClient();
    const r = await c.put(uniqueKey(), "ok");
    expect(r.error).toBeUndefined();
  });

  it("derived token works for its bucket", async () => {
    const bucket = uniqueBucket();
    await ensureBucket(bucket);
    const token = await deriveToken(getRootToken(), bucket);
    const c = createObjectsClient({ url: getTestUrl(), token, bucket });
    const r = await c.put(uniqueKey(), "ok");
    expect(r.error).toBeUndefined();
  });

  it("derived token does NOT work for another bucket", async () => {
    const bucketA = uniqueBucket();
    const bucketB = uniqueBucket();
    await ensureBucket(bucketA);
    await ensureBucket(bucketB);
    const tokenA = await deriveToken(getRootToken(), bucketA);
    const c = createObjectsClient({
      url: getTestUrl(),
      token: tokenA,
      bucket: bucketB,
    });
    const r = await c.put(uniqueKey(), "ok");
    expect(r.error?.code).toBe("unauthorized");
    expect(r.response.status).toBe(401);
  });

  it("invalid token returns 401", async () => {
    const c = createObjectsClient({
      url: getTestUrl(),
      token: "not-a-real-token",
    });
    const r = await c.put(uniqueKey(), "x");
    expect(r.response.status).toBe(401);
  });
});

describe("auth — construction errors", () => {
  let savedUrl: string | undefined;
  let savedToken: string | undefined;

  beforeEach(() => {
    savedUrl = process.env["OBJECTS_URL"];
    savedToken = process.env["OBJECTS_ROOT_TOKEN"];
    delete process.env["OBJECTS_URL"];
    delete process.env["OBJECTS_ROOT_TOKEN"];
  });

  afterEach(() => {
    if (savedUrl !== undefined) process.env["OBJECTS_URL"] = savedUrl;
    if (savedToken !== undefined) {
      process.env["OBJECTS_ROOT_TOKEN"] = savedToken;
    }
  });

  it("constructing without a token throws", () => {
    expect(() => createObjectsClient({ url: "http://localhost:1" })).toThrow(
      /token/i,
    );
  });

  it("constructing without a url throws", () => {
    expect(() => createObjectsClient({ token: "x" })).toThrow(/OBJECTS_URL/);
  });
});

describe("deriveToken", () => {
  it("matches HMAC-SHA256 hex computed in Node", async () => {
    const root = "root-token-xyz";
    const bucket = "photos";
    const expected = createHmac("sha256", root).update(bucket).digest("hex");
    const got = await deriveToken(root, bucket);
    expect(got).toBe(expected);
  });

  it("returns a 64-char lowercase hex string", async () => {
    const sig = await deriveToken("k", "b");
    expect(sig).toMatch(/^[0-9a-f]{64}$/);
  });

  it("is deterministic across calls", async () => {
    const a = await deriveToken("k", "b");
    const b = await deriveToken("k", "b");
    expect(a).toBe(b);
  });

  it("matches the server's HMAC scheme via live request", async () => {
    const bucket = uniqueBucket();
    await ensureBucket(bucket);
    const token = await deriveToken(getRootToken(), bucket);
    const c = createObjectsClient({ url: getTestUrl(), token, bucket });
    const r = await c.list();
    expect(r.error).toBeUndefined();
  });
});
