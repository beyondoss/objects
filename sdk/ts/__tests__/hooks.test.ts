import { describe, expect, it } from "vitest";
import { createObjectsClient } from "../src/index.js";
import { getRootToken, getTestUrl, uniqueKey } from "./harness.js";

describe("onRequest / onResponse hooks", () => {
  it("fires onRequest before and onResponse after each command", async () => {
    const requests: string[] = [];
    const responses: { command: string; durationMs: number }[] = [];
    const c = createObjectsClient({
      url: getTestUrl(),
      token: getRootToken(),
      onRequest: ({ command }) => requests.push(command),
      onResponse: (event) => responses.push(event),
    });

    const key = uniqueKey();
    await c.put(key, "hookable");
    const got = await c.get(key);
    await got.data?.cancel();
    await c.head(key);
    await c.delete(key);

    expect(requests).toEqual(["put", "get", "head", "delete"]);
    expect(responses.map((r) => r.command)).toEqual([
      "put",
      "get",
      "head",
      "delete",
    ]);
    for (const r of responses) {
      expect(r.durationMs).toBeGreaterThanOrEqual(0);
      expect(Number.isFinite(r.durationMs)).toBe(true);
    }
  });

  it("fires hooks for buckets.* commands with namespaced labels", async () => {
    const seen: string[] = [];
    const c = createObjectsClient({
      url: getTestUrl(),
      token: getRootToken(),
      onRequest: ({ command }) => seen.push(command),
    });

    const name = `b${Date.now().toString(36)}${
      Math.random().toString(36).slice(2, 6)
    }`;
    await c.buckets.create(name);
    await c.buckets.list();
    await c.buckets.get(name);
    await c.buckets.update(name, { access: "public" });
    await c.buckets.delete(name);

    expect(seen).toEqual([
      "buckets.create",
      "buckets.list",
      "buckets.get",
      "buckets.update",
      "buckets.delete",
    ]);
  });

  it("onResponse fires even when the command errors", async () => {
    const responses: { command: string; durationMs: number }[] = [];
    const c = createObjectsClient({
      url: getTestUrl(),
      token: getRootToken(),
      onResponse: (event) => responses.push(event),
    });

    const { error } = await c.get(uniqueKey());
    expect(error?.code).toBe("object_not_found");
    expect(responses).toHaveLength(1);
    expect(responses[0]!.command).toBe("get");
  });
});
