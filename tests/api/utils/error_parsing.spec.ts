import {
  AzureEvalErrorParser,
  BigqueryEvalErrorParser,
  RedshiftEvalErrorParser
} from "@dataform/api/utils/error_parsing";
import { expect } from "chai";
import { suite, test } from "df/testing";

suite("error_parsing", () => {
  suite("bigquery", () => {
    test("successfully extracts line and column", () => {
      const SAMPLE_BIGQUERY_ERROR = {
        message: "Syntax error: Unexpected identifier 'asda' at [2:1]"
      };
      const parsedError = BigqueryEvalErrorParser(SAMPLE_BIGQUERY_ERROR);
      expect(parsedError.errorLocation).to.deep.equal({ line: 2, column: 1 });
    });

    test("if no message, return", () => {
      const SAMPLE_BIGQUERY_ERROR = {
        message: ""
      };
      const parsedError = BigqueryEvalErrorParser(SAMPLE_BIGQUERY_ERROR);
      expect(parsedError.errorLocation).to.be.undefined;
    });

    test("if too many matches, return", () => {
      const SAMPLE_BIGQUERY_ERROR = {
        message: "Syntax error: Unexpected identifier '[3:4]' at [2:1]"
      };
      const parsedError = BigqueryEvalErrorParser(SAMPLE_BIGQUERY_ERROR);
      expect(parsedError.errorLocation).to.be.undefined;
    });
  });

  suite("redshift", () => {
    test("successfully extracts line", () => {
      const SAMPLE_REDSHIFT_ERROR = {
        message: "some message",
        position: "11"
      };
      const SAMPLE_REDSHIFT_STATEMENT = `\nsomething\nasda\n123`;
      const parsedError = RedshiftEvalErrorParser(SAMPLE_REDSHIFT_STATEMENT, SAMPLE_REDSHIFT_ERROR);
      expect(parsedError.errorLocation).to.deep.equal({ line: 2 });
    });

    test("if no position, return", () => {
      const SAMPLE_REDSHIFT_ERROR = {
        message: "some message",
        position: ""
      };
      const SAMPLE_REDSHIFT_STATEMENT = `\nsomething\nasda\n123`;
      const parsedError = RedshiftEvalErrorParser(SAMPLE_REDSHIFT_STATEMENT, SAMPLE_REDSHIFT_ERROR);
      expect(parsedError.errorLocation).to.be.undefined;
    });
  });

  suite("redshift", () => {
    test("successfully extracts line", () => {
      const SAMPLE_REDSHIFT_ERROR = {
        message: "some message",
        position: "11"
      };
      const SAMPLE_REDSHIFT_STATEMENT = `\nsomething\nasda\n123`;
      const parsedError = RedshiftEvalErrorParser(SAMPLE_REDSHIFT_STATEMENT, SAMPLE_REDSHIFT_ERROR);
      expect(parsedError.errorLocation).to.deep.equal({ line: 2 });
    });

    test("if no position, return", () => {
      const SAMPLE_REDSHIFT_ERROR = {
        message: "some message",
        position: ""
      };
      const SAMPLE_REDSHIFT_STATEMENT = `\nsomething\nasda\n123`;
      const parsedError = RedshiftEvalErrorParser(SAMPLE_REDSHIFT_STATEMENT, SAMPLE_REDSHIFT_ERROR);
      expect(parsedError.errorLocation).to.be.undefined;
    });
  });

  suite("azure", () => {
    test("successfully extracts line", () => {
      const SAMPLE_AZURE_ERROR = {
        originalError: {
          info: {
            message: "Parse error at line: 14, column: 13: Incorrect syntax near 'current_date'."
          }
        }
      };
      const parsedError = AzureEvalErrorParser(SAMPLE_AZURE_ERROR);
      expect(parsedError.errorLocation).to.deep.equal({ line: 14, column: 13 });
    });

    test("if no message, return", () => {
      const SAMPLE_AZURE_ERROR = {
        originalError: {
          info: {}
        }
      };
      const parsedError = AzureEvalErrorParser(SAMPLE_AZURE_ERROR);
      expect(parsedError.errorLocation).to.be.undefined;
    });
  });
});
