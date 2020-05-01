import { expect } from "chai";
import {
  parseAzureEvaluationError,
  parseBigqueryEvalError,
  parseRedshiftEvalError
} from "df/api/utils/error_parsing";
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

  suite("redshift", () => {
    test("successfully extracts line", () => {
      const SAMPLE_REDSHIFT_ERROR = {
        message: "some message",
        position: "12"
      };
      const SAMPLE_REDSHIFT_STATEMENT = `\nsomething\nasda\n123`;
      const parsedError = parseRedshiftEvalError(SAMPLE_REDSHIFT_STATEMENT, SAMPLE_REDSHIFT_ERROR);
      expect(parsedError.errorLocation).to.deep.equal({ line: 3, column: 2 });
    });

    test("if no position, return", () => {
      const SAMPLE_REDSHIFT_ERROR = {
        message: "some message",
        position: ""
      };
      const SAMPLE_REDSHIFT_STATEMENT = `\nsomething\nasda\n123`;
      const parsedError = parseRedshiftEvalError(SAMPLE_REDSHIFT_STATEMENT, SAMPLE_REDSHIFT_ERROR);
      expect(parsedError.errorLocation).equals(null);
    });
  });

  suite("azure", () => {
    test("successfully extracts line", () => {
      const SAMPLE_AZURE_ERROR = {
        originalError: {
          info: {
            message: "Parse error at line: 14, column: 13: Incorrect syntax near '234'."
          }
        }
      };
      const parsedError = parseAzureEvaluationError(SAMPLE_AZURE_ERROR);
      expect(parsedError.errorLocation).to.deep.equal({ line: 14, column: 13 });
    });

    test("if no message, return", () => {
      const SAMPLE_AZURE_ERROR = {
        originalError: {
          info: {}
        }
      };
      const parsedError = parseAzureEvaluationError(SAMPLE_AZURE_ERROR);
      expect(parsedError.errorLocation).equals(null);
    });
  });
});
