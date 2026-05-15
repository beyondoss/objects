import { createObjectsClient, type ObjectsClient } from "./client.js";

let _objects: ObjectsClient | undefined;

/**
 * Default Objects client configured from environment variables.
 * Reads `BEYOND_OBJECTS_URL` (required) and `BEYOND_OBJECTS_ROOT_TOKEN` (required).
 * Initialized lazily on first method call.
 */
export const objects: ObjectsClient = new Proxy({} as ObjectsClient, {
  get(_, prop) {
    _objects ??= createObjectsClient();
    return (_objects as unknown as Record<string | symbol, unknown>)[prop];
  },
});

export {
  type Access,
  type Bucket,
  type BucketsClient,
  type Camelize,
  type CopyResult,
  type CreateBucketOptions,
  createObjectsClient,
  createS3Credentials as deriveS3Credentials,
  deriveToken,
  type GetOptions,
  type HeadResult,
  type ListOptions,
  type ListResult,
  type ObjectItem,
  type ObjectsClient,
  type ObjectsClientOptions,
  type ObjectsRequestEvent,
  type ObjectsResponseEvent,
  type ObjectsResult,
  type PutBody,
  type PutOptions,
  type PutResult,
  type Range,
  type S3Credentials,
  type TlsOptions,
  type UpdateBucketOptions,
  type UploadTokenResult,
} from "./client.js";
export { ObjectsError } from "./errors.js";
export type { components, operations, paths } from "./types.js";
