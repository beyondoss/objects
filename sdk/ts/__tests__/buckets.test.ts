import { describe, expect, it } from "vitest";
import { rootClient, uniqueBucket, uniqueKey } from "./harness.js";

describe("buckets — CRUD", () => {
  it("create + get + list + delete round-trip", async () => {
    const c = rootClient();
    const name = uniqueBucket();

    const created = await c.buckets.create(name, { access: "private" });
    expect(created.error).toBeUndefined();
    expect(created.data?.name).toBe(name);
    expect(created.data?.access).toBe("private");

    const got = await c.buckets.get(name);
    expect(got.error).toBeUndefined();
    expect(got.data?.name).toBe(name);

    const all = await c.buckets.list();
    expect(all.error).toBeUndefined();
    expect(all.data?.some((b) => b.name === name)).toBe(true);

    const del = await c.buckets.delete(name);
    expect(del.error).toBeUndefined();

    const after = await c.buckets.get(name);
    expect(after.response.status).toBe(404);
  });

  it("create is idempotent", async () => {
    const c = rootClient();
    const name = uniqueBucket();
    const a = await c.buckets.create(name);
    const b = await c.buckets.create(name);
    expect(a.error).toBeUndefined();
    expect(b.error).toBeUndefined();
    expect(a.data?.name).toBe(b.data?.name);
    await c.buckets.delete(name);
  });

  it("update changes the access default", async () => {
    const c = rootClient();
    const name = uniqueBucket();
    await c.buckets.create(name, { access: "private" });

    const updated = await c.buckets.update(name, { access: "public" });
    expect(updated.error).toBeUndefined();
    expect(updated.data?.access).toBe("public");

    await c.buckets.delete(name);
  });

  it("delete on a non-empty bucket fails with bucket_not_empty", async () => {
    const c = rootClient();
    const name = uniqueBucket();
    await c.buckets.create(name);

    const scoped = rootClient({ bucket: name });
    await scoped.put(uniqueKey(), "x");

    const del = await c.buckets.delete(name);
    expect(del.error?.code).toBe("bucket_not_empty");

    await scoped.delete((await scoped.list()).data!.objects[0]!.key);
    await c.buckets.delete(name);
  });

  it("delete is idempotent — 404 returns no error", async () => {
    const c = rootClient();
    const name = uniqueBucket();
    const first = await c.buckets.delete(name);
    expect(first.error).toBeUndefined();
  });
});
