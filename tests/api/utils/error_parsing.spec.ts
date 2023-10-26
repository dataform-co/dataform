import { expect } from "chai";

import { parseBigqueryEvalError } from "df/cli/api/utils/error_parsing";
import { suite, test } from "df/testing";

suite("error_parsing", () => {
  suite("bigquery", () => {
    test("successfully extracts line and column", () => {
      const SAMPLE_BIGQUERY_ERROR = {
        message: "Syntax error: Unexpected identifier '[3:4]' at [2:1]"
      };
      const parsedError = parseBigqueryEvalError(SAMPLE_BIGQUERY_ERROR);
      expect(parsedError.errorLocation).to.deep.equal({ line: 2, column: 1 });
    });

    test("if no message, return", () => {
      const SAMPLE_BIGQUERY_ERROR = {
        message: ""
      };
      const parsedError = parseBigqueryEvalError(SAMPLE_BIGQUERY_ERROR);
      expect(parsedError.errorLocation).equals(null);
    });
  });
});
