import { expect } from "chai";

import { parseBigqueryEvalError, shiftErrorPositionLines } from "df/cli/api/utils/error_parsing";
import { suite, test } from "df/testing";

suite("error_parsing", () => {
  suite("bigquery", () => {
    test("successfully extracts line and column", () => {
      const SAMPLE_BIGQUERY_ERROR = {
        message: "Syntax error: Unexpected identifier '[3:4]' at [2:1]",
      };
      const parsedError = parseBigqueryEvalError(SAMPLE_BIGQUERY_ERROR);
      expect(parsedError.errorLocation).to.deep.equal({ line: 2, column: 1 });
    });

    test("if no message, return", () => {
      const SAMPLE_BIGQUERY_ERROR = {
        message: "",
      };
      const parsedError = parseBigqueryEvalError(SAMPLE_BIGQUERY_ERROR);
      expect(parsedError.errorLocation).equals(null);
    });

    test("shifts the lines of error positions", () => {
      expect(
        shiftErrorPositionLines("Query error: Unrecognized name: x at [4:64]; also at [1:2]", 2),
      ).equals("Query error: Unrecognized name: x at [6:64]; also at [3:2]");
    });

    test("doesn't shift bracketed numbers that aren't error positions", () => {
      expect(
        shiftErrorPositionLines("Syntax error: Unexpected identifier '[3:4]' at [2:1]", 2),
      ).equals("Syntax error: Unexpected identifier '[3:4]' at [4:1]");
    });

    test("leaves messages without positions unchanged", () => {
      expect(shiftErrorPositionLines("Access Denied: Table p:d.t", 2)).equals(
        "Access Denied: Table p:d.t",
      );
    });
  });
});
