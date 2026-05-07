import { describe, expect, it } from "vitest";
import { dec, readAll, rootClient, uniqueKey } from "./harness.js";

const ALPHABET = "abcdefghijklmnopqrstuvwxyz";

describe("range requests", () => {
  it("returns 206 with Content-Range for `{ start, end }`", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, ALPHABET);

    const { data, response, error } = await c.get(key, {
      range: { start: 5, end: 9 },
    });
    expect(error).toBeUndefined();
    expect(response.status).toBe(206);
    expect(response.headers.get("content-range")).toBe(
      `bytes 5-9/${ALPHABET.length}`,
    );
    expect(response.headers.get("content-length")).toBe("5");
    expect(dec(await readAll(data!))).toBe("fghij");
  });

  it("supports open-ended `{ start }` (to end of object)", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, ALPHABET);

    const { data, response } = await c.get(key, { range: { start: 23 } });
    expect(response.status).toBe(206);
    expect(dec(await readAll(data!))).toBe("xyz");
  });

  it("supports `{ suffix }` (last N bytes)", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, ALPHABET);

    const { data, response } = await c.get(key, { range: { suffix: 4 } });
    expect(response.status).toBe(206);
    expect(dec(await readAll(data!))).toBe("wxyz");
  });

  it("absent range returns 200 with the full body", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, ALPHABET);

    const { data, response } = await c.get(key);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-range")).toBeNull();
    expect(dec(await readAll(data!))).toBe(ALPHABET);
  });

  it("range past end of object returns 416 range_not_satisfiable", async () => {
    const c = rootClient();
    const key = uniqueKey();
    await c.put(key, ALPHABET);

    const { error, response } = await c.get(key, {
      range: { start: 1000, end: 2000 },
    });
    expect(response.status).toBe(416);
    expect(error?.code).toBe("range_not_satisfiable");
  });

  it("range round-trips arbitrary slices of binary content", async () => {
    const c = rootClient();
    const key = uniqueKey();
    const payload = new Uint8Array(1024);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    await c.put(key, payload);

    for (
      const r of [
        { start: 0, end: 0 },
        { start: 256, end: 511 },
        { suffix: 1 },
        { start: 1023 },
      ] as const
    ) {
      const { data, response } = await c.get(key, { range: r });
      expect(response.status).toBe(206);
      const slice = await readAll(data!);
      const expected = "suffix" in r
        ? payload.slice(payload.length - r.suffix)
        : payload.slice(r.start, (r.end ?? payload.length - 1) + 1);
      expect(slice).toEqual(expected);
    }
  });
});
