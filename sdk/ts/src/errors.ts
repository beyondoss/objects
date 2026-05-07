/**
 * Returned when the Objects service returns a non-2xx response.
 *
 * @example
 * ```ts
 * try {
 *   await objects.put('avatar.png', file)
 * } catch (err) {
 *   if (err instanceof ObjectsError) {
 *     console.error(err.code, err.message)
 *   }
 * }
 * ```
 */
export class ObjectsError extends Error {
  readonly code: string;
  readonly status: number;
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
