import { expect } from "chai";

import { endsWithStatementTerminator } from "df/cli/api/utils/sql_parsing";
import { suite, test } from "df/testing";

suite("endsWithStatementTerminator", () => {
  test("detects a trailing semicolon, ignoring whitespace", () => {
    expect(endsWithStatementTerminator("SELECT 1;")).equals(true);
    expect(endsWithStatementTerminator("SELECT 1;  \n\t")).equals(true);
    expect(endsWithStatementTerminator("SELECT 1")).equals(false);
    expect(endsWithStatementTerminator("")).equals(false);
  });

  test("ignores trailing comments", () => {
    expect(endsWithStatementTerminator("SELECT 1; -- comment")).equals(true);
    expect(endsWithStatementTerminator("SELECT 1; # comment")).equals(true);
    expect(endsWithStatementTerminator("SELECT 1; /* comment */")).equals(true);
    expect(endsWithStatementTerminator("SELECT 1 -- comment;")).equals(false);
    expect(endsWithStatementTerminator("SELECT 1 /* comment; */")).equals(false);
    expect(endsWithStatementTerminator("SELECT 1;\n-- comment\n-- another")).equals(true);
  });

  test("ignores comment markers and semicolons inside string literals and identifiers", () => {
    expect(endsWithStatementTerminator("SELECT '--' AS a")).equals(false);
    expect(endsWithStatementTerminator("SELECT 'a;'")).equals(false);
    expect(endsWithStatementTerminator('SELECT "#;"')).equals(false);
    expect(endsWithStatementTerminator("SELECT '''a\n-- b;\n'''")).equals(false);
    expect(endsWithStatementTerminator('SELECT """a;"""')).equals(false);
    expect(endsWithStatementTerminator("SELECT * FROM `p.d.t--;`")).equals(false);
    expect(endsWithStatementTerminator("SELECT 'it\\'s; -- x';")).equals(true);
    expect(endsWithStatementTerminator("SELECT '--';")).equals(true);
  });

  test("handles multi-statement scripts", () => {
    expect(endsWithStatementTerminator("DECLARE x INT64;\nSET x = 1;\nSELECT x")).equals(false);
    expect(
      endsWithStatementTerminator("BEGIN\n  SELECT 1;\nEXCEPTION WHEN ERROR THEN\n  RAISE;\nEND;"),
    ).equals(true);
  });
});
