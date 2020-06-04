import { expect } from "chai";

import { SyntaxTreeNode, SyntaxTreeNodeType } from "df/sqlx/lexer";
import { suite, test } from "df/testing";

suite("@dataform/sqlx", () => {
  suite("syntax tree construction", () => {
    test("SQL strings don't affect the tree", () => {
      const actual = SyntaxTreeNode.create("SELECT SUM(IF(track.event = 'example', 1, 0)) js { }");
      const expected = new SyntaxTreeNode(SyntaxTreeNodeType.SQL, [
        "SELECT SUM(IF(track.event = ",
        new SyntaxTreeNode(SyntaxTreeNodeType.SQL_LITERAL_STRING, ["'example'"]),
        ", 1, 0)) ",
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
        "select regexp(",
        new SyntaxTreeNode(SyntaxTreeNodeType.SQL_LITERAL_STRING, ['"^/([0-9]+)\\"/.*"']),
        ", ",
        new SyntaxTreeNode(SyntaxTreeNodeType.JAVASCRIPT_TEMPLATE_STRING_PLACEHOLDER, [
          '${ref("dab")}'
        ]),
        ")"
      ]);
      expect(actual.equals(expected)).equals(true);
    });
  });
});
