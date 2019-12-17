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
    it("replaces single backslash with double backslashes in pre operation", () => {
      const parsedSqlx = parseSqlx(
        "pre_operations {SELECT regexp_extract('01a_data_engine', '^(\\d{2}\\w)')}"
      );
      expect(parsedSqlx.preOperations).eql([
        "SELECT regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')"
      ]);
    });
    it("closing braces in inner sql quotes doesn't close block", () => {
      const parsedSqlx = parseSqlx("pre_operations {SELECT '{}'}");
      expect(parsedSqlx.preOperations).eql(["SELECT '{}'"]);
    });
    it("closing braces in inner sql quotes with backslashes after doesn't close block", () => {
      const parsedSqlx = parseSqlx("pre_operations {SELECT '\\d{2}'}");
      expect(parsedSqlx.preOperations).eql(["SELECT '\\\\d{2}'"]);
    });
    it("escaped quotes do not close single quoted string", () => {
      const parsedSqlx = parseSqlx("SELECT 'asd\\'123\"def'");
      expect(parsedSqlx.sql).eql(["SELECT 'asd\\\\'123\"def'"]);
    });
    it("escaped quotes do not close double quoted string", () => {
      const parsedSqlx = parseSqlx('SELECT "asd\\"123\'def"');
      expect(parsedSqlx.sql).eql(['SELECT "asd\\\\"123\'def"']);
    });
  });
});
