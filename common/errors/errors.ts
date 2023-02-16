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

export function coerceAsError<T extends Error | any>(error: T): T extends Error ? T : Error {
  // If it's an error instance, return it.
  if ((error as any) instanceof Error) {
    return error as any;
  }
  const errorLike = error as any;
  // Otherwise, attempt to reconstruct an error class from the object.
  const message = errorLike.message ? String(errorLike.message) : String(errorLike);
  const coercedError = new Error(message);
  if (errorLike.stack) {
    coercedError.stack = errorLike.stack;
  }
  if (errorLike.name) {
    coercedError.name = errorLike.name;
  }
  return coercedError as any;
}
