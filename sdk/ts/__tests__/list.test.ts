import { describe, expect, it } from "vitest";
import { rootClient, uniqueBucket } from "./harness.js";

describe("list — prefix scan and pagination", () => {
  it("returns objects in ascending key order", async () => {
    const bucket = uniqueBucket();
    const c = rootClient({ bucket });
    await c.buckets.create(bucket);

    const keys = ["a/x", "a/y", "a/z", "b/x", "c/x"];
    for (const k of keys) await c.put(k, k);

    const { data, error } = await c.list();
    expect(error).toBeUndefined();
    expect(data?.objects.map((o) => o.key)).toEqual(keys);
  });

  it("filters by prefix", async () => {
    const bucket = uniqueBucket();
    const c = rootClient({ bucket });
    await c.buckets.create(bucket);
    await c.put("logs/a", "1");
    await c.put("logs/b", "2");
    await c.put("imgs/x", "3");

    const { data } = await c.list({ prefix: "logs/" });
    expect(data?.objects.map((o) => o.key)).toEqual(["logs/a", "logs/b"]);
  });

  it("paginates via cursor", async () => {
    const bucket = uniqueBucket();
    const c = rootClient({ bucket });
    await c.buckets.create(bucket);
    for (let i = 0; i < 5; i++) await c.put(`p/${i}`, "x");

    const first = await c.list({ prefix: "p/", limit: 2 });
    expect(first.data?.objects.length).toBe(2);
    expect(first.data?.nextCursor).toBeDefined();

    const second = await c.list({
      prefix: "p/",
      limit: 2,
      cursor: first.data!.nextCursor,
    });
    expect(second.data?.objects.length).toBe(2);

    const third = await c.list({
      prefix: "p/",
      limit: 2,
      cursor: second.data!.nextCursor,
    });
    expect(third.data?.objects.length).toBe(1);
    expect(third.data?.nextCursor).toBeUndefined();
  });

  it("returns camelized fields (nextCursor, lastModified, contentType)", async () => {
    const c = rootClient();
    const key = `cm/${Date.now()}-${Math.random()}`;
    await c.put(key, "x", { contentType: "text/plain" });
    const { data } = await c.list({ prefix: "cm/", limit: 1000 });
    const found = data?.objects.find((o) => o.key === key);
    expect(found).toBeDefined();
    expect(typeof found!.lastModified).toBe("string");
    expect(found!.contentType).toBe("text/plain");
  });

  it("empty bucket returns no objects", async () => {
    const bucket = uniqueBucket();
    const c = rootClient({ bucket });
    await c.buckets.create(bucket);
    const { data } = await c.list();
    expect(data?.objects).toEqual([]);
  });
});
