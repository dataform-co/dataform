export class ErrorWithCause extends Error {
  constructor(message?: string | Error, public readonly cause?: Error) {
    super(typeof message === "string" ? message : undefined);
    if (typeof message !== "string") {
      this.cause = message;
    }
    if (this.cause) {
      this.stack += `\nCaused by: ${this.cause.stack}`;
    }
  }
}

export function coerceAsError<T extends Error | any>(errorLike: T): T extends Error ? T : Error {
  // If it's an error instance, return it.
  if ((errorLike as any) instanceof Error) {
    return errorLike as any;
  }
  const error = errorLike as any;
  // Otherwise, attempt to reconstruct an error class from the object.
  const message = error.message || error.error || (typeof errorLike === "string" ? errorLike : JSON.stringify(errorLike));
  const coercedError = new Error(String(message));
  if (error.stack) {
    coercedError.stack = error.stack;
  }
  if (error.name) {
    coercedError.name = error.name;
  }
  return coercedError as any;
}
