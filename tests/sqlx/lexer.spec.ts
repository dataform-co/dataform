import { constructSyntaxTree, parseSqlx } from "@dataform/sqlx/lexer";
import { expect } from "chai";

describe("@dataform/sqlx", () => {
  describe("lexer", () => {
    it("replaces single backslash with double backslashes", () => {
      // These look like groups of 2 backslashes, but are actually 1. If someone
      // in SQLX SQL writes "\" in the regex, it should be interpreted as "\".
      const parsedSqlx = parseSqlx("select regexp_extract('01a_data_engine', '^(\\d{2}\\w)')");
      expect(parsedSqlx.sql).eql(["select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')"]);
    });
    it("replaces double backslashes with quadruple backslashes", () => {
      // In SQLX SQL, "\\" should be interpreted as "\\".
      const parsedSqlx = parseSqlx("select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')");
      expect(parsedSqlx.sql).eql([
        "select regexp_extract('01a_data_engine', '^(\\\\\\\\d{2}\\\\\\\\w)')"
      ]);
    });
    it("replaces single backslash with double backslashes in pre operation", () => {
      const parsedSqlx = parseSqlx(
        "pre_operations {select regexp_extract('01a_data_engine', '^(\\d{2}\\w)')}"
      );
      expect(parsedSqlx.preOperations).eql([
        "select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')"
      ]);
    });
    it("closing braces in inner sql quotes doesn't close block", () => {
      const parsedSqlx = parseSqlx("pre_operations {select '{}'}");
      expect(parsedSqlx.preOperations).eql(["select '{}'"]);
    });
    it("closing braces in inner sql quotes with backslashes after doesn't close block", () => {
      const parsedSqlx = parseSqlx("pre_operations {select '\\d{2}'}");
      expect(parsedSqlx.preOperations).eql(["select '\\\\d{2}'"]);
    });
    it("escaped quotes do not close single quoted string", () => {
      const parsedSqlx = parseSqlx("select 'asd\\'123\"def'");
      expect(parsedSqlx.sql).eql(["select 'asd\\\\'123\"def'"]);
    });
    it("escaped quotes do not close double quoted string", () => {
      const parsedSqlx = parseSqlx('select "asd\\"123\'def"');
      expect(parsedSqlx.sql).eql(['select "asd\\\\"123\'def"']);
    });
    it("js block in quoted string is interpreted just as a string", () => {
      const parsedSqlx = parseSqlx(
        'select * from regexp_extract(\'js {\', "") js { const freaky_stuff = "js {"}'
      );
      expect(parsedSqlx.sql).eql(["select * from regexp_extract('js {', \"\")"]);
    });
  });
});
