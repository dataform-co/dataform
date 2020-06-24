import { expect } from "chai";
import { basename } from "path";

import { coerceAsError, ErrorWithCause } from "df/common/errors/errors";
import { suite, test } from "df/testing";

suite(basename(__filename), () => {
  suite("ErrorWithCause", () => {
    for (const testCase of [
      { name: "with message", message: "an error was thrown!" },
      { name: "without message" }
    ]) {
      test(`by default, acts the same as Error, ${testCase.name}`, () => {
        const errorWithCause = new ErrorWithCause(testCase.message);
        const normalError = new Error(testCase.message);
        expect(errorWithCause.message).eql(normalError.message);
        expect(errorWithCause.stack).eql(
          normalError.stack.replace(
            /errors\.spec\.ts:\d{1,3}:\d{1,3}/,
            errorWithCause.stack.match(/errors\.spec\.ts:\d{1,3}:\d{1,3}/)[0]
          )
        );
        expect(errorWithCause.toString()).eql(normalError.toString());
      });
    }

    test("stack includes cause stack", () => {
      const cause = new Error("some other error");
      const errorWithCause = new ErrorWithCause("top-level error", cause);

      // Full stacktrace should be double the length of a single Error's stacktrace.
      expect(errorWithCause.stack.split("\n").length).eql(cause.stack.split("\n").length * 2);
      expect(errorWithCause.stack.startsWith("Error: top-level error")).eql(true);
      expect(errorWithCause.stack.endsWith(cause.stack)).eql(true);
    });
  });

  suite("coerceAsError", () => {
    test("preserves original error", () => {
      const originalError = new ErrorWithCause(new Error("cause"));
      const coercedError = coerceAsError(originalError);

      expect(coercedError).equals(originalError);
    });

    test("coerces error like objects", () => {
      const originalError = {
        message: "message",
        stack: "stack",
        name: "name"
      };
      const coercedError = coerceAsError(originalError);

      expect(coercedError.message).equals("message");
      expect(coercedError.stack).equals("stack");
      expect(coercedError.name).equals("name");
    });

    test("coerces non error types", () => {
      const originalError = "string";
      const coercedError = coerceAsError(originalError);

      expect(coercedError.message).equals("string");
      expect(coercedError instanceof Error).equals(true);
    });
  });
});
