import { constructSyntaxTree, parseSqlx } from "@dataform/sqlx/lexer";
import { expect } from "chai";

describe("@dataform/sqlx", () => {
  describe("lexer", () => {
    it("replaces single backslash with double backslashes", () => {
      // These look like groups of 2 backslashes, but are actually 1. If someone
      // in SQLX SQL writes "\" in the regex, it should be interpreted as "\".
      const parsedSqlx = parseSqlx("SELECT regexp_extract('01a_data_engine', '^(\\d{2}\\w)')");
      expect(parsedSqlx.sql).eql(["SELECT regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')"]);
    });
    it("replaces double backslashes with quadruple backslashes", () => {
      // In SQLX SQL, "\\" should be interpreted as "\\".
      const parsedSqlx = parseSqlx("SELECT regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')");
      expect(parsedSqlx.sql).eql([
        "SELECT regexp_extract('01a_data_engine', '^(\\\\\\\\d{2}\\\\\\\\w)')"
      ]);
    });
  });
});
