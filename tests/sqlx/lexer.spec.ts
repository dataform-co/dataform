import { parseSqlx, SyntaxTreeNode, SyntaxTreeNodeType } from "@dataform/sqlx/lexer";
import { suite, test } from "@dataform/testing";
import { expect } from "chai";

suite("@dataform/sqlx", () => {
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
