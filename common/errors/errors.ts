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
