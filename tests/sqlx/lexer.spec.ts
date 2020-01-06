import { constructSyntaxTree, parseSqlx } from "@dataform/sqlx/lexer";
import { expect } from "chai";

describe("@dataform/sqlx", () => {
  describe("outer SQL lexing", () => {
    it("backslashes are duplicated, so that they act literally when included in a JavaScript template string", () => {
      // If someone in SQLX SQL writes "\" in the regex, it should be
      // interpreted as "\".
      const tests: Array<{ in: string; expected: string }> = [
        {
          // These look like groups of 2 backslashes, but are actually 1 (javascript).
          in: "select regexp_extract('01a_data_engine', '^(\\d{2}\\w)')",
          expected: "select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')"
        },
        {
          in: "select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')",
          expected: "select regexp_extract('01a_data_engine', '^(\\\\\\\\d{2}\\\\\\\\w)')"
        },
        {
          in: "select * from regexp_extract('\\\\', '')",
          expected: "select * from regexp_extract('\\\\\\\\', '')"
        },
        {
          in: 'select from regexp_extract("", r"[0-9]\\"*")',
          expected: 'select from regexp_extract("", r"[0-9]\\\\"*")'
        }
      ];
      tests.forEach(test => {
        expect(parseSqlx(test.in).sql).eql([test.expected]);
      });
    });
    it("nothing in a string is interpreted as a special term", () => {
      const tests: Array<{ in: string; expected: string }> = [
        { in: 'select "asd\\"123\'def"', expected: 'select "asd\\\\"123\'def"' },
        { in: "select 'asd\\'123\"def'", expected: "select 'asd\\\\'123\"def'" },
        {
          in: "select * from regexp_extract('js {', \"\")",
          expected: "select * from regexp_extract('js {', \"\")"
        }
      ];
      tests.forEach(test => {
        expect(parseSqlx(test.in).sql).eql([test.expected]);
      });
    });
    it("javascript placeholder tokenized", () => {
      expect(parseSqlx("select * from ${ref('dab')}").sql).eql(["select * from ${ref('dab')}"]);
    });
  });
  describe("inner SQL lexing", () => {
    it("backslashes are duplicated, so that they act literally when included in a JavaScript template string", () => {
      const tests: Array<{ in: string; expected: string }> = [
        {
          in: "pre_operations {select regexp_extract('01a_data_engine', '^(\\d{2}\\w)')}",
          expected: "select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')"
        },
        {
          in: "pre_operations {select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')}",
          expected: "select regexp_extract('01a_data_engine', '^(\\\\\\\\d{2}\\\\\\\\w)')"
        },
        {
          in: "pre_operations {select * from regexp_extract('\\\\', '')}",
          expected: "select * from regexp_extract('\\\\\\\\', '')"
        },
        { in: "pre_operations {}", expected: "" }
      ];
      tests.forEach(test => {
        expect(parseSqlx(test.in).preOperations).eql([test.expected]);
      });
    });
    it("nothing in a string is interpreted as a special term", () => {
      const tests: Array<{ in: string; expected: string }> = [
        { in: 'post_operations {select "asd\'123"}', expected: 'select "asd\'123"' },
        { in: "post_operations {select 'asd\"123'}", expected: "select 'asd\"123'" },
        {
          in: "post_operations {select * from regexp_extract('js {', \"\")}",
          expected: "select * from regexp_extract('js {', \"\")"
        }
      ];
      tests.forEach(test => {
        expect(parseSqlx(test.in).postOperations).eql([test.expected]);
      });
    });
  });
  describe("syntax tree construction", () => {
    it("SQL strings don't affect the tree", () => {
      const tree = constructSyntaxTree("SELECT SUM(IF(track.event = 'example', 1, 0)) js { }");
      const expected = {
        contentType: "sql",
        contents: [
          "SELECT SUM(IF(track.event = 'example', 1, 0)) ",
          { contentType: "js", contents: ["js { }"] }
        ]
      };
      expect(tree).eql(expected);
    });
    it("inline js blocks tokenized", () => {
      const tree = constructSyntaxTree("select * from ${ref('dab')}");
      const expected = {
        contentType: "sql",
        contents: ["select * from ", { contentType: "jsPlaceholder", contents: ["${ref('dab')}"] }]
      };
      expect(tree).eql(expected);
    });
    it("inline js blocks tokenized correctly if string present beforehand", () => {
      const tree = constructSyntaxTree('select regexp("^/([0-9]+)\\"/.*", ${ref("dab")})');
      const expected = {
        contentType: "sql",
        contents: [
          'select regexp("^/([0-9]+)\\"/.*", ',
          { contentType: "jsPlaceholder", contents: ['${ref("dab")}'] },
          ")"
        ]
      };
      expect(tree).eql(expected);
    });
  });
  describe("whitespace parsing", () => {
    it("whitespace not required after JS blocks at end of file.", () => {
      expect(parseSqlx("select ${TEST}\njs {\n    const TEST = 'test';\n}").js).eql(
        "\n    const TEST = 'test';\n"
      );
    });
  });
});
