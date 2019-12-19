import { constructSyntaxTree, parseSqlx } from "@dataform/sqlx/lexer";
import { expect } from "chai";

describe("@dataform/sqlx", () => {
  describe("outer SQL lexing", () => {
    it("backslashes are duplicated so that they act as written in the IDE", () => {
      // If someone in SQLX SQL writes "\" in the regex, it should be
      // interpreted as "\".
      const pairs = [
        [
          // These look like groups of 2 backslashes, but are actually 1 (javascript).
          "select regexp_extract('01a_data_engine', '^(\\d{2}\\w)')",
          "select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')"
        ],
        [
          "select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')",
          "select regexp_extract('01a_data_engine', '^(\\\\\\\\d{2}\\\\\\\\w)')"
        ],
        ["select * from regexp_extract('\\\\', '')", "select * from regexp_extract('\\\\\\\\', '')"]
      ];
      pairs.forEach(pair => {
        expect(parseSqlx(pair[0]).sql).eql([pair[1]]);
      });
    });
    it("nothing in a string is interpreted as a special term", () => {
      const pairs = [
        ['select "asd\\"123\'def"', 'select "asd\\\\"123\'def"'],
        ["select 'asd\\'123\"def'", "select 'asd\\\\'123\"def'"],
        ["select * from regexp_extract('js {', \"\")", "select * from regexp_extract('js {', \"\")"]
      ];
      pairs.forEach(pair => {
        expect(parseSqlx(pair[0]).sql).eql([pair[1]]);
      });
    });
  });
  describe("inner SQL lexing", () => {
    it("backslashes are duplicated so that they act as written in the IDE", () => {
      const pairs = [
        [
          "pre_operations {select regexp_extract('01a_data_engine', '^(\\d{2}\\w)')}",
          "select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')"
        ],
        [
          "pre_operations {select regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)')}",
          "select regexp_extract('01a_data_engine', '^(\\\\\\\\d{2}\\\\\\\\w)')"
        ],
        [
          "pre_operations {select * from regexp_extract('\\\\', '')}",
          "select * from regexp_extract('\\\\\\\\', '')"
        ],
        ["pre_operations {}", ""]
      ];
      pairs.forEach(pair => {
        expect(parseSqlx(pair[0]).preOperations).eql([pair[1]]);
      });
    });
    it("nothing in a string is interpreted as a special term", () => {
      const pairs = [
        ['post_operations {select "asd\'123"}', 'select "asd\'123"'],
        ["post_operations {select 'asd\"123'}", "select 'asd\"123'"],
        [
          "post_operations {select * from regexp_extract('js {', \"\")}",
          "select * from regexp_extract('js {', \"\")"
        ]
      ];
      pairs.forEach(pair => {
        expect(parseSqlx(pair[0]).postOperations).eql([pair[1]]);
      });
    });
  });
});
