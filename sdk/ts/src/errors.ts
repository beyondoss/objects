/**
 * Returned in the `error` field when the Objects service responds with a non-2xx.
 *
 * @example
 * ```ts
 * const { error } = await objects.put("avatar.png", file)
 * if (error instanceof ObjectsError) {
 *   console.error(error.code, error.message)
 * }
 * ```
 */
export class ObjectsError extends Error {
  readonly code: string;
  readonly status: number;
  /**
   * The raw HTTP response that produced this error, when available.
   * Useful for inspecting headers (`x-amz-meta-*`, `Retry-After`, `Content-Range`,
   * `WWW-Authenticate`) on a failure path — particularly after a caller has
   * `throw`n the error and lost the `{ response }` field of the result tuple.
   *
   * `undefined` only for errors thrown before a request was sent
   * (e.g. missing `OBJECTS_URL`).
   */
  readonly response: Response | undefined;
  readonly hint: string | undefined;

  constructor(
    code: string,
    message: string,
    status: number,
    response?: Response,
    hint?: string,
  ) {
    super(message);
    this.name = "ObjectsError";
    this.code = code;
    this.status = status;
    this.response = response;
    this.hint = hint;
  }
}
