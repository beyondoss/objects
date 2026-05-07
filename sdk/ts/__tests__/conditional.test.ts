import { describe, expect, it } from "vitest";
import { rootClient, uniqueKey } from "./harness.js";

describe("conditional writes (CAS)", () => {
  it("ifNoneMatch: \"*\" succeeds when the object does not exist", async () => {
    const c = rootClient();
    const key = uniqueKey();
    const r = await c.put(key, "first", { ifNoneMatch: "*" });
    expect(r.error).toBeUndefined();
  });

  it("ifNoneMatch: \"*\" fails with 412 when the object exists", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "first");
    const r = await c.put(key, "second", { ifNoneMatch: "*" });
    expect(r.response.status).toBe(412);
    expect(r.error?.code).toBe("object_exists");
  });

  it("ifMatch succeeds when the etag matches", async () => {
    const c = rootClient();
    const key = uniqueKey();
    const { data } = await c.put(key, "v1");
    const r = await c.put(key, "v2", { ifMatch: data!.etag });
    expect(r.error).toBeUndefined();
  });

  it("ifMatch fails with 412 when the etag is stale", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, "v1");
    const r = await c.put(key, "v2", {
      ifMatch:
        "\"0000000000000000000000000000000000000000000000000000000000000000\"",
    });
    expect(r.response.status).toBe(412);
    expect(r.error?.code).toBe("etag_mismatch");
  });
});
