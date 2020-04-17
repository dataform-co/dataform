import { parseSqlx, SyntaxTreeNode, SyntaxTreeNodeType } from "@dataform/sqlx/lexer";
import { suite, test } from "@dataform/testing";
import { expect } from "chai";

suite("@dataform/sqlx", () => {
  suite("outer SQL lexing", () => {
    test("backslashes are duplicated, so that they act literally when included in a JavaScript template string", () => {
      // If someone in SQLX SQL writes "\" in the regex, it should be
      // interpreted as "\".
      const testCases: Array<{ in: string; expected: string }> = [
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
      testCases.forEach(testCase => {
        expect(parseSqlx(testCase.in).sql).eql([testCase.expected]);
      });
    });
    test("nothing in a string is interpreted as a special term", () => {
      const testCases: Array<{ in: string; expected: string }> = [
        { in: 'select "asd\\"123\'def"', expected: 'select "asd\\\\"123\'def"' },
        { in: "select 'asd\\'123\"def'", expected: "select 'asd\\\\'123\"def'" },
        {
          in: "select * from regexp_extract('js {', \"\")",
          expected: "select * from regexp_extract('js {', \"\")"
        }
      ];
      testCases.forEach(testCase => {
        expect(parseSqlx(testCase.in).sql).eql([testCase.expected]);
      });
    });
    test("javascript placeholder tokenized", () => {
      expect(parseSqlx("select * from ${ref('dab')}").sql).eql(["select * from ${ref('dab')}"]);
    });
  });
  suite("inner SQL lexing", () => {
    test("backslashes are duplicated, so that they act literally when included in a JavaScript template string", () => {
      const testCases: Array<{ in: string; expected: string }> = [
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
      testCases.forEach(testCase => {
        expect(parseSqlx(testCase.in).preOperations).eql([testCase.expected]);
      });
    });
    test("nothing in a string is interpreted as a special term", () => {
      const testCases: Array<{ in: string; expected: string }> = [
        { in: 'post_operations {select "asd\'123"}', expected: 'select "asd\'123"' },
        { in: "post_operations {select 'asd\"123'}", expected: "select 'asd\"123'" },
        {
          in: "post_operations {select * from regexp_extract('js {', \"\")}",
          expected: "select * from regexp_extract('js {', \"\")"
        }
      ];
      testCases.forEach(testCase => {
        expect(parseSqlx(testCase.in).postOperations).eql([testCase.expected]);
      });
    });
  });
  suite("syntax tree construction", () => {
    test("SQL strings don't affect the tree", () => {
      const actual = SyntaxTreeNode.create("SELECT SUM(IF(track.event = 'example', 1, 0)) js { }");
      const expected = new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
        "SELECT SUM(IF(track.event = 'example', 1, 0)) ",
        new SyntaxTreeNode(SyntaxTreeNodeType.JAVASCRIPT, ["js { }"])
      ]);
      expect(actual.equals(expected)).equals(true);
    });
    test("inline js blocks tokenized", () => {
      const actual = SyntaxTreeNode.create("select * from ${ref('dab')}");
      const expected = new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
        "select * from ",
        new SyntaxTreeNode(SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER, [
          "${ref('dab')}"
        ])
      ]);
      expect(actual.equals(expected)).equals(true);
    });
    test("inline js blocks tokenized correctly if string present beforehand", () => {
      const actual = SyntaxTreeNode.create('select regexp("^/([0-9]+)\\"/.*", ${ref("dab")})');
      const expected = new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
        'select regexp("^/([0-9]+)\\"/.*", ',
        new SyntaxTreeNode(SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER, [
          '${ref("dab")}'
        ]),
        ")"
      ]);
      expect(actual.equals(expected)).equals(true);
    });
  });
  suite("whitespace parsing", () => {
    test("whitespace not required after JS blocks at end of file.", () => {
      expect(parseSqlx("select ${TEST}\njs {\n    const TEST = 'test';\n}").js).eql(
        "\n    const TEST = 'test';\n"
      );
    });
  });
});
