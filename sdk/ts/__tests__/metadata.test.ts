import { describe, expect, it } from "vitest";
import { rootClient, uniqueKey } from "./harness.js";

describe("user metadata", () => {
  it("round-trips metadata via head", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "with-meta", {
      contentType: "text/plain",
      metadata: { owner: "jared", project: "objects", "trace-id": "abc-123" },
    });

    const { data, error } = await c.head(key);
    expect(error).toBeUndefined();
    expect(data?.metadata).toEqual({
      owner: "jared",
      project: "objects",
      "trace-id": "abc-123",
    });
  });

  it("metadata is also exposed on get response headers", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "x", { metadata: { region: "us-west-2" } });

    const { response, data } = await c.get(key);
    expect(response.headers.get("x-amz-meta-region")).toBe("us-west-2");
    await data!.cancel();
  });

  it("absent metadata is an empty object on head", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "no-meta");

    const { data } = await c.head(key);
    expect(data?.metadata).toEqual({});
  });

  it("metadata keys are lowercased on the wire", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "case", {
      metadata: { TraceId: "t-1", REGION: "us-east-1" },
    });

    const { data } = await c.head(key);
    expect(data?.metadata).toEqual({ traceid: "t-1", region: "us-east-1" });
  });

  it("setAccess preserves user metadata", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "stable", { metadata: { kind: "image" } });

    await c.setAccess(key, "public");
    const { data } = await c.head(key);
    expect(data?.access).toBe("public");
    expect(data?.metadata).toEqual({ kind: "image" });
  });
});
