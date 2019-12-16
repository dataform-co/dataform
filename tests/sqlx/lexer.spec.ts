import { constructSyntaxTree, parseSqlx, lexer } from "@dataform/sqlx/lexer";
import { expect } from "chai";

describe("@dataform/sqlx", () => {

  describe("lexer", () => {
    it("sqlx_single_backslash_stays_as_double", () => {
      // These look like groups of 2 backslashes, but are actually 1. If someone
      // in SQLX SQL writes "/" in the regex, it should be interpreted as "/".
      const testCase = "SELECT regexp_extract('01a_data_engine', '^(\\d{2}\\w)')";
      const parsedTestCase = parseSqlx(testCase);
      const expectedResult = "SELECT regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')";
      expect(parsedTestCase.sql[0]).equals(expectedResult);
    });
    it("sqlx_double_backslash_stays_as_double", () => {
      // In SQLX SQL, "//" should be interpreted as "//".
      const testCase = "SELECT regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')";
      const parsedTestCase = parseSqlx(testCase);
      const expectedResult = "SELECT regexp_extract('01a_data_engine', '^(\\\\\\\\d{2}\\\\\\\\w)')";
      expect(parsedTestCase.sql[0]).equals(expectedResult);
    });
  });
});