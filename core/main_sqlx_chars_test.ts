import { expect } from "chai";

import { suite, test, writeDefinitionFile, writeWorkflowSettingsFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  WorkflowSettingsTemplates
} from "df/testing/run_core";

suite("sqlx special characters", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  test("extract blocks", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, WorkflowSettingsTemplates.bigquery);
    writeDefinitionFile(
      projectDir,
      "file.sqlx",
      `
config {
  type: "table"
}
js {
  var a = 1;
}
/*
A multiline comment
*/
pre_operations {
  SELECT 2;
}
post_operations {
  SELECT 3;
}
-- A single line comment.
SELECT \${a}`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.tables[0].query).equals(`


/*
A multiline comment
*/


-- A single line comment.
SELECT 1`);
    expect(result.compile.compiledGraph.tables[0].preOps[0]).equals(`
  SELECT 2;
`);
    expect(result.compile.compiledGraph.tables[0].postOps[0]).equals(`
  SELECT 3;
`);
  });

  test("backticks appear to users as written", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, WorkflowSettingsTemplates.bigquery);
    const fileContents = `select
  "\`",
  """\`"",
from \`location\``;
    writeDefinitionFile(projectDir, "file.sqlx", fileContents);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.operations[0].queries[0]).equals(fileContents);
  });

  test("backslashes appear to users as written", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, WorkflowSettingsTemplates.bigquery);
    const sqlContents = `select
  regexp_extract('01a_data_engine', '^(\\d{2}\\w)'),
  regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)'),
  regexp_extract('\\\\', ''),
  regexp_extract("", r"[0-9]\\"*"),
  """\\ \\? \\\\"""`;
    writeDefinitionFile(
      projectDir,
      "file.sqlx",
      `config { type: "table" }` + sqlContents + `pre_operations { ${sqlContents} }`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.tables[0].query.trim()).equals(sqlContents);
    expect(result.compile.compiledGraph.tables[0].preOps[0].trim()).equals(sqlContents);
  });

  test("strings appear to users as written", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, WorkflowSettingsTemplates.bigquery);
    const sqlContents = `select
"""
triple
quotes
""",
"asd\\"123'def",
'asd\\'123"def',

select
"""
triple
quotes
""",
"asd\\"123'def",
'asd\\'123"def'`;
    writeDefinitionFile(
      projectDir,
      "file.sqlx",
      `config { type: "table" }` + sqlContents + `post_operations { ${sqlContents} }`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.tables[0].query.trim()).equals(sqlContents);
    expect(result.compile.compiledGraph.tables[0].postOps[0].trim()).equals(sqlContents);
  });
});
