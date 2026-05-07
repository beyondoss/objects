import { ObjectsError } from "./errors.js";

export type ObjectsClient = {
  put(
    key: string,
    body: unknown,
    options?: PutOptions,
  ): Promise<{ url: string }>;
  get(key: string): Promise<ReadableStream>;
  head(key: string): Promise<HeadResult>;
  delete(key: string): Promise<void>;
  move(from: string, to: string): Promise<void>;
  copy(from: string, to: string): Promise<void>;
  list(options?: ListOptions): Promise<ListResult>;
};

export type PutOptions = {
  contentType?: string;
  access?: "public" | "private";
  ifNoneMatch?: "*";
  ifMatch?: string;
};

export type HeadResult = {
  size: number;
  etag: string;
  contentType: string;
  access: "public" | "private";
  lastModified: Date;
};

export type ObjectItem = {
  key: string;
  size: number;
  etag: string;
  contentType: string;
  access: "public" | "private";
  lastModified: Date;
  url: string;
};

export type ListOptions = {
  prefix?: string;
  cursor?: string;
  limit?: number;
};

export type ListResult = {
  objects: ObjectItem[];
  nextCursor?: string;
};

export type CreateClientOptions = {
  url?: string;
  key?: string;
  bucket?: string;
};

export function createObjectsClient(
  _options?: CreateClientOptions,
): ObjectsClient {
  throw new ObjectsError("not_implemented", "not yet implemented", 501);
}

export function deriveKey(_rootKey: string, _bucket: string): string {
  throw new ObjectsError("not_implemented", "not yet implemented", 501);
}
