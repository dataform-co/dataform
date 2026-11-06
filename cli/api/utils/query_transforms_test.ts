import { expect } from "chai";

import {
  concatenateQueries,
  setsDatasetId,
  terminateStatements,
  wrapWithCreateSchemaIfNotExists,
} from "df/cli/api/utils/query_transforms";
import { suite, test } from "df/testing";

suite("concatenateQueries", () => {
  test("adds semicolons to non-last statements and joins with newlines", () => {
    expect(concatenateQueries(["SELECT 1", "SELECT 2"])).deep.equals("SELECT 1;\nSELECT 2");
  });

  test("does not add semicolon to the last statement", () => {
    expect(concatenateQueries(["SELECT 1", "SELECT 2", "SELECT 3"])).deep.equals(
      "SELECT 1;\nSELECT 2;\nSELECT 3",
    );
  });

  test("preserves existing semicolons on non-last statements", () => {
    expect(concatenateQueries(["SELECT 1;", "SELECT 2"])).deep.equals("SELECT 1;\nSELECT 2");
  });

  test("preserves existing semicolons on last statement", () => {
    expect(concatenateQueries(["SELECT 1;", "SELECT 2;"])).deep.equals("SELECT 1;\nSELECT 2;");
  });

  test("adds semicolon before trailing comment if necessary", () => {
    expect(
      concatenateQueries(["SELECT 1 -- blah", "drop table foo; -- baz", "SELECT 2"]),
    ).deep.equals("SELECT 1; -- blah\ndrop table foo; -- baz\nSELECT 2");
  });

  test("preserves comment-only lines", () => {
    expect(concatenateQueries(["SELECT 1;", "--drop table foo", "SELECT 2", "--baz"])).deep.equals(
      "SELECT 1;\n--drop table foo\nSELECT 2\n--baz",
    );
  });

  test("does not add semicolon to last code statement when followed only by comments", () => {
    expect(concatenateQueries(["SELECT 1", "-- trailing comment", "/* another */"])).deep.equals(
      "SELECT 1\n-- trailing comment\n/* another */",
    );
  });

  test("doesn't mistake -- inside a string literal for a trailing comment", () => {
    expect(concatenateQueries(["DELETE FROM t WHERE note = '--'", "SELECT 1"])).deep.equals(
      "DELETE FROM t WHERE note = '--';\nSELECT 1",
    );
  });

  test("adds semicolon before trailing # comment", () => {
    expect(concatenateQueries(["SELECT 1 # hash comment", "SELECT 2"])).deep.equals(
      "SELECT 1; # hash comment\nSELECT 2",
    );
  });

  test("doesn't mistake comment markers or semicolons inside strings and identifiers", () => {
    const terminated = (statement: string) => concatenateQueries([statement, "SELECT 2"]);
    expect(terminated("SELECT 'a;'")).equals("SELECT 'a;';\nSELECT 2");
    expect(terminated('SELECT "# not a comment"')).equals('SELECT "# not a comment";\nSELECT 2');
    expect(terminated("SELECT '''a\n-- b;\n'''")).equals("SELECT '''a\n-- b;\n''';\nSELECT 2");
    expect(terminated("SELECT * FROM `p.d.t--`")).equals("SELECT * FROM `p.d.t--`;\nSELECT 2");
    expect(terminated("SELECT 'it\\'s -- x'")).equals("SELECT 'it\\'s -- x';\nSELECT 2");
  });

  test("doesn't treat \\ as an escape inside raw string or bytes literals", () => {
    const terminated = (statement: string) => concatenateQueries([statement, "SELECT 2"]);
    expect(terminated("SELECT r'\\' -- comment")).equals("SELECT r'\\'; -- comment\nSELECT 2");
    expect(terminated("SELECT REPLACE(p, r'\\', '/'), 'ok' -- don't fail")).equals(
      "SELECT REPLACE(p, r'\\', '/'), 'ok'; -- don't fail\nSELECT 2",
    );
    expect(terminated('R"\\" -- comment')).equals('R"\\"; -- comment\nSELECT 2');
    expect(terminated("SELECT r'''a\\''' -- comment")).equals(
      "SELECT r'''a\\'''; -- comment\nSELECT 2",
    );
    expect(terminated("SELECT rb'\\' -- comment")).equals("SELECT rb'\\'; -- comment\nSELECT 2");
    expect(terminated('SELECT BR"\\" -- comment')).equals('SELECT BR"\\"; -- comment\nSELECT 2');
    // Non-raw `b` prefix and SQL keywords ending in `r` still escape quotes with `\`.
    expect(terminated("SELECT b'it\\'s -- x'")).equals("SELECT b'it\\'s -- x';\nSELECT 2");
    expect(terminated("SELECT flag OR'it\\'s -- x'")).equals(
      "SELECT flag OR'it\\'s -- x';\nSELECT 2",
    );
  });

  test("adds semicolon before trailing block comment", () => {
    expect(concatenateQueries(["SELECT 1 /* comment; */", "SELECT 2"])).equals(
      "SELECT 1; /* comment; */\nSELECT 2",
    );
  });

  test("doesn't add semicolon after a terminated script", () => {
    const script = "BEGIN\n  SELECT 1;\nEXCEPTION WHEN ERROR THEN\n  RAISE;\nEND;";
    expect(concatenateQueries([script, "SELECT 2"])).equals(`${script}\nSELECT 2`);
  });

  test("adds semicolon after the code when the statement ends with several comment lines", () => {
    expect(concatenateQueries(["SELECT 1 -- first\n-- second", "SELECT 2"])).deep.equals(
      "SELECT 1; -- first\n-- second\nSELECT 2",
    );
  });

  test("preserves single line comments within a query", () => {
    expect(concatenateQueries(["SELECT 1\n--test\nFROM UNNEST([]);"])).deep.equals(
      "SELECT 1\n--test\nFROM UNNEST([]);",
    );
  });

  test("preserves multi line comments within a query", () => {
    expect(
      concatenateQueries(["SELECT 1\n\*multi\nline\ncomment\n*/\nFROM UNNEST([]);"]),
    ).deep.equals("SELECT 1\n\*multi\nline\ncomment\n*/\nFROM UNNEST([]);");
  });

  test("trims whitespace from statements", () => {
    expect(concatenateQueries(["  SELECT 1  ", "  SELECT 2  ", "  SELECT 3 -- blah"])).deep.equals(
      "SELECT 1;\nSELECT 2;\nSELECT 3 -- blah",
    );
  });

  test("filters out empty and falsy statements", () => {
    expect(concatenateQueries(["SELECT 1", "", "SELECT 2"])).deep.equals("SELECT 1;\nSELECT 2");
    expect(concatenateQueries(["SELECT 1", undefined, "SELECT 2"])).deep.equals(
      "SELECT 1;\nSELECT 2",
    );
  });

  test("keeps leading whitespace if trimLeadingWhitespace is false", () => {
    expect(
      concatenateQueries(["SELECT 1", "\n\n  SELECT 2  \n", "  \n  "], undefined, false),
    ).deep.equals("SELECT 1;\n\n\n  SELECT 2");
  });

  test("returns empty string for empty input", () => {
    expect(concatenateQueries([])).deep.equals("");
  });

  test("returns single statement without semicolon", () => {
    expect(concatenateQueries(["SELECT 1"])).deep.equals("SELECT 1");
  });

  test("applies modifier to code portion of each statement", () => {
    expect(concatenateQueries(["SELECT 1", "SELECT 2"], (stmt) => `(${stmt})`)).deep.equals(
      "(SELECT 1);\n(SELECT 2)",
    );
  });

  test("applies modifier only to code portion, not comment", () => {
    expect(
      concatenateQueries(["SELECT 1 --comment", "SELECT 2"], (stmt) => `(${stmt})`),
    ).deep.equals("(SELECT 1); --comment\n(SELECT 2)");
  });
});

suite("wrapWithCreateSchemaIfNotExists", () => {
  test("wraps a statement in a block after creating the schema", () => {
    expect(wrapWithCreateSchemaIfNotExists("project", "dataset", "SELECT 1;")).equals(
      "create schema if not exists `project.dataset`;\nbegin\nSELECT 1;\nend;",
    );
  });

  test("adds a missing semicolon", () => {
    expect(wrapWithCreateSchemaIfNotExists("project", "dataset", "  SELECT 1\n")).equals(
      "create schema if not exists `project.dataset`;\nbegin\nSELECT 1;\nend;",
    );
  });

  test("terminates a statement with a trailing comment", () => {
    expect(wrapWithCreateSchemaIfNotExists("project", "dataset", "SELECT 1 -- comment")).equals(
      "create schema if not exists `project.dataset`;\nbegin\nSELECT 1; -- comment\nend;",
    );
  });

  test("terminates a raw string ending in a backslash before a trailing comment", () => {
    expect(wrapWithCreateSchemaIfNotExists("project", "dataset", "SELECT r'\\' -- comment")).equals(
      "create schema if not exists `project.dataset`;\nbegin\nSELECT r'\\'; -- comment\nend;",
    );
  });

  test("keeps leading DECLARE statements at the start of the block", () => {
    expect(
      wrapWithCreateSchemaIfNotExists("project", "dataset", "DECLARE x INT64;\nSELECT x"),
    ).equals(
      "create schema if not exists `project.dataset`;\nbegin\nDECLARE x INT64;\nSELECT x;\nend;",
    );
  });

  test("omits the database if not set", () => {
    expect(wrapWithCreateSchemaIfNotExists(undefined, "dataset", "SELECT 1;")).equals(
      "create schema if not exists `dataset`;\nbegin\nSELECT 1;\nend;",
    );
  });
});

suite("setsDatasetId", () => {
  test("detects SET @@dataset_id statements", () => {
    expect(setsDatasetId("SET @@dataset_id = 'my_ds'")).equals(true);
    expect(setsDatasetId('set @@DATASET_ID="my_ds";\nSELECT * FROM t')).equals(true);
    expect(setsDatasetId("DECLARE x INT64;\nSET\n  @@dataset_id = 'my_ds';")).equals(true);
    expect(setsDatasetId("SET /* comment */ @@dataset_id = 'my_ds'")).equals(true);
    expect(setsDatasetId("SET -- comment\n@@dataset_id = 'my_ds'")).equals(true);
    expect(setsDatasetId("SET (@@dataset_project_id, @@dataset_id) = ('proj', 'my_ds')")).equals(
      true,
    );
  });

  test("ignores SET @@dataset_id inside comments, string literals, and quoted identifiers", () => {
    expect(setsDatasetId("SELECT 1 -- SET @@dataset_id = 'my_ds'")).equals(false);
    expect(setsDatasetId("SELECT 1 # SET @@dataset_id = 'my_ds'")).equals(false);
    expect(setsDatasetId("/* SET @@dataset_id = 'my_ds' */\nSELECT 1")).equals(false);
    expect(setsDatasetId("SELECT 'SET @@dataset_id = \"my_ds\"'")).equals(false);
    expect(setsDatasetId("SELECT \"SET @@dataset_id = 'my_ds'\"")).equals(false);
    expect(setsDatasetId("SELECT '''\nSET @@dataset_id = 'my_ds'\n'''")).equals(false);
    expect(setsDatasetId("SELECT r'\\' -- not a comment\n, 'SET @@dataset_id = \"my_ds\"'")).equals(
      false,
    );
    expect(setsDatasetId("SELECT * FROM `SET @@dataset_id = 'my_ds'`")).equals(false);
  });

  test("ignores reading @@dataset_id or setting other variables", () => {
    expect(setsDatasetId("SELECT @@dataset_id")).equals(false);
    expect(setsDatasetId("SET x = @@dataset_id")).equals(false);
    expect(setsDatasetId("SET (x, y) = (@@dataset_id, 1)")).equals(false);
    expect(setsDatasetId("SET @@dataset_project_id = 'proj'")).equals(false);
    expect(setsDatasetId("SET @@dataset_id_other = 'my_ds'")).equals(false);
  });
});

suite("terminateStatements", () => {
  test("terminates every statement including the last one", () => {
    expect(terminateStatements(["SELECT 1", "SELECT 2"])).equals("SELECT 1;\nSELECT 2;");
  });

  test("places semicolons before trailing comments and skips empty statements", () => {
    expect(terminateStatements(["  SELECT 1 -- comment  ", "", "SELECT 2;"])).equals(
      "SELECT 1; -- comment\nSELECT 2;",
    );
  });

  test("returns empty string for empty input", () => {
    expect(terminateStatements([])).equals("");
  });
});
