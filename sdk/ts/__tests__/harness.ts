import { randomUUID } from "node:crypto";
import {
  createObjectsClient,
  deriveToken,
  type ObjectsClient,
  type ObjectsClientOptions,
} from "../src/index.js";

export function getTestUrl(): string {
  const url = process.env["OBJECTS_TEST_URL"];
  if (url == null) throw new Error("OBJECTS_TEST_URL is not set");
  return url;
}

export function getRootToken(): string {
  const t = process.env["OBJECTS_TEST_ROOT_TOKEN"];
  if (t == null) throw new Error("OBJECTS_TEST_ROOT_TOKEN is not set");
  return t;
}

export function rootClient(
  overrides?: Partial<ObjectsClientOptions>,
): ObjectsClient {
  return createObjectsClient({
    url: getTestUrl(),
    token: getRootToken(),
    ...overrides,
  });
}

export async function bucketClient(bucket: string): Promise<ObjectsClient> {
  await ensureBucket(bucket);
  const token = await deriveToken(getRootToken(), bucket);
  return createObjectsClient({ url: getTestUrl(), token, bucket });
}

export async function ensureBucket(bucket: string): Promise<void> {
  const admin = rootClient();
  const { error, response } = await admin.buckets.create(bucket);
  if (error != null && response.status !== 200 && response.status !== 201) {
    throw new Error(
      `failed to ensure bucket ${bucket}: ${error.code} ${error.message}`,
    );
  }
}

export function uniqueKey(prefix = "k"): string {
  return `${prefix}-${randomUUID()}`;
}

export function uniqueBucket(): string {
  return `b${randomUUID().replace(/-/g, "").slice(0, 12)}`;
}

export const enc = (s: string): Uint8Array => new TextEncoder().encode(s);
export const dec = (b: Uint8Array): string => new TextDecoder().decode(b);

export async function readAll(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value != null) {
      chunks.push(value);
      total += value.byteLength;
    }
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.byteLength;
  }
  return out;
}
