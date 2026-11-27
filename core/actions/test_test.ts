import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML,
} from "df/testing/run_core";

suite("test", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test(`test with no inputs`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const definitionsDir = path.join(projectDir, "definitions");
    const actionsYamlPath = path.join(definitionsDir, "actions.yaml");
    const actionSqlPath = path.join(definitionsDir, "action.sql");
    const actionTestSqlxPath = path.join(definitionsDir, "action_test.sqlx");

    fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(definitionsDir);
    fs.writeFileSync(
      actionsYamlPath,
      `
actions:
- table:
    filename: action.sql`,
    );
    fs.writeFileSync(actionSqlPath, "SELECT 1");
    fs.writeFileSync(
      actionTestSqlxPath,
      `
config {
  type: "test",
  dataset: "action",
  tags: ["tag1", "tag2"]
}
SELECT 1`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.tests)).deep.equals(
      asPlainObject([
        {
          // Original test properties
          name: "action_test",
          testQuery: "SELECT 1",
          expectedOutputQuery: "\n\nSELECT 1",
          fileName: "definitions/action_test.sqlx",
          tags: ["tag1", "tag2"],
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "action_test",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "action_test",
          },
        },
      ]),
    );
    expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
      asPlainObject([
        {
          target: {
            database: "defaultProject",
            name: "action",
            schema: "defaultDataset",
          },
          canonicalTarget: {
            database: "defaultProject",
            name: "action",
            schema: "defaultDataset",
          },
          disabled: false,
          enumType: "TABLE",
          fileName: "definitions/action.sql",
          hermeticity: "NON_HERMETIC",
          query: "SELECT 1",
          type: "table",
        },
      ]),
    );
  });

  test(`test with multiple_inputs input`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const definitionsDir = path.join(projectDir, "definitions");
    const actionsYamlPath = path.join(definitionsDir, "actions.yaml");
    const action1SqlxPath = path.join(definitionsDir, "action1.sqlx");
    const action1TestSqlxPath = path.join(definitionsDir, "action1_test.sqlx");
    const action2SqlxPath = path.join(definitionsDir, "action2.sqlx");
    const action2TestSqlxPath = path.join(definitionsDir, "action2_test.sqlx");

    fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(definitionsDir);

    // Add a declaration
    fs.writeFileSync(
      actionsYamlPath,
      `
actions:
- declaration:
    name: a_declaration`,
    );

    // Add an action with a test, reads from declaration
    fs.writeFileSync(
      action1SqlxPath,
      `
config {
  type: "table",
}
SELECT a,b,c FROM \${ref("a_declaration")}
    `,
    );
    fs.writeFileSync(
      action1TestSqlxPath,
      `
config {
  type: "test",
  dataset: "action1"
}
input "a_declaration" {
  SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d
}
SELECT 1 AS a, 2 AS b, 3 AS c`,
    );

    // Add an action with a test, reads from previous action
    fs.writeFileSync(
      action2SqlxPath,
      `
config {
  type: "table",
}
SELECT a,b FROM \${ref("action1")}
    `,
    );
    fs.writeFileSync(
      action2TestSqlxPath,
      `
config {
  type: "test",
  dataset: "action2"
}
input "action1" {
  SELECT 1 AS a, 2 AS b, 3 AS c
}
SELECT 1 AS a, 2 AS b`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.tests)).deep.equals(
      asPlainObject([
        {
          // Original test properties
          name: "action1_test",
          testQuery: "\n\nSELECT a,b,c FROM (\n  SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d\n)\n    ",
          expectedOutputQuery: "\n\n\nSELECT 1 AS a, 2 AS b, 3 AS c",
          fileName: "definitions/action1_test.sqlx",
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "action1_test",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "action1_test",
          },
        },
        {
          // Original test properties
          name: "action2_test",
          testQuery: "\n\nSELECT a,b FROM (\n  SELECT 1 AS a, 2 AS b, 3 AS c\n)\n    ",
          expectedOutputQuery: "\n\n\nSELECT 1 AS a, 2 AS b",
          fileName: "definitions/action2_test.sqlx",
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "action2_test",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "action2_test",
          },
        },
      ]),
    );
    expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
      asPlainObject([
        {
          target: {
            database: "defaultProject",
            name: "action1",
            schema: "defaultDataset",
          },
          canonicalTarget: {
            database: "defaultProject",
            name: "action1",
            schema: "defaultDataset",
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              name: "a_declaration",
              schema: "defaultDataset",
            },
          ],
          disabled: false,
          enumType: "TABLE",
          fileName: "definitions/action1.sqlx",
          hermeticity: "NON_HERMETIC",
          query: "\n\nSELECT a,b,c FROM `defaultProject.defaultDataset.a_declaration`\n    ",
          type: "table",
        },
        {
          target: {
            database: "defaultProject",
            name: "action2",
            schema: "defaultDataset",
          },
          canonicalTarget: {
            database: "defaultProject",
            name: "action2",
            schema: "defaultDataset",
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              name: "action1",
              schema: "defaultDataset",
            },
          ],
          disabled: false,
          enumType: "TABLE",
          fileName: "definitions/action2.sqlx",
          hermeticity: "NON_HERMETIC",
          query: "\n\nSELECT a,b FROM `defaultProject.defaultDataset.action1`\n    ",
          type: "table",
        },
      ]),
    );
  });

  test(`test with two actions with same name and different schema`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const definitionsDir = path.join(projectDir, "definitions");
    const schema1actionSqlxPath = path.join(definitionsDir, "schema1_action.sqlx");
    const schema2actionSqlxPath = path.join(definitionsDir, "schema2_action.sqlx");
    const schema1actionTestSqlxPath = path.join(definitionsDir, "schema1_action_test.sqlx");
    const schema2actionTestSqlxPath = path.join(definitionsDir, "schema2_action_test.sqlx");

    fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(definitionsDir);
    fs.writeFileSync(
      schema1actionSqlxPath,
      `
config {
  schema: "schema1",
  name: "action",
  type: "table",
}
SELECT 1
    `,
    );
    fs.writeFileSync(
      schema2actionSqlxPath,
      `
config {
  schema: "schema2",
  name: "action",
  type: "table",
}
SELECT 2
    `,
    );
    fs.writeFileSync(
      schema1actionTestSqlxPath,
      `
config {
  type: "test",
  dataset: {
    schema: "schema1",
    name: "action",
  },
}
SELECT 1`,
    );
    fs.writeFileSync(
      schema2actionTestSqlxPath,
      `
config {
  type: "test",
  dataset: {
    schema: "schema2",
    name: "action",
  },
}
SELECT 2`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.tests)).deep.equals(
      asPlainObject([
        {
          // Original test properties
          name: "schema1_action_test",
          testQuery: "\n\nSELECT 1\n    ",
          expectedOutputQuery: "\n\nSELECT 1",
          fileName: "definitions/schema1_action_test.sqlx",

          // New properties
          target: {
            database: "defaultProject",
            schema: "schema1",
            name: "schema1_action_test",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "schema1",
            name: "schema1_action_test",
          },
        },
        {
          // Original test properties
          name: "schema2_action_test",
          testQuery: "\n\nSELECT 2\n    ",
          expectedOutputQuery: "\n\nSELECT 2",
          fileName: "definitions/schema2_action_test.sqlx",

          // New properties
          target: {
            database: "defaultProject",
            schema: "schema2",
            name: "schema2_action_test",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "schema2",
            name: "schema2_action_test",
          },
        },
      ]),
    );
    expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
      asPlainObject([
        {
          target: {
            database: "defaultProject",
            name: "action",
            schema: "schema1",
          },
          canonicalTarget: {
            database: "defaultProject",
            name: "action",
            schema: "schema1",
          },
          disabled: false,
          enumType: "TABLE",
          fileName: "definitions/schema1_action.sqlx",
          hermeticity: "NON_HERMETIC",
          query: "\n\nSELECT 1\n    ",
          type: "table",
        },
        {
          target: {
            database: "defaultProject",
            name: "action",
            schema: "schema2",
          },
          canonicalTarget: {
            database: "defaultProject",
            name: "action",
            schema: "schema2",
          },
          disabled: false,
          enumType: "TABLE",
          fileName: "definitions/schema2_action.sqlx",
          hermeticity: "NON_HERMETIC",
          query: "\n\nSELECT 2\n    ",
          type: "table",
        },
      ]),
    );
  });

  test(`test with empty test sql`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const definitionsDir = path.join(projectDir, "definitions");
    const actionsYamlPath = path.join(definitionsDir, "actions.yaml");
    const action1SqlxPath = path.join(definitionsDir, "action1.sqlx");
    const action1TestSqlxPath = path.join(definitionsDir, "action1_test.sqlx");
    const action2SqlxPath = path.join(definitionsDir, "action2.sqlx");
    const action2TestSqlxPath = path.join(definitionsDir, "action2_test.sqlx");

    fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(definitionsDir);

    // Add a declaration
    fs.writeFileSync(
      actionsYamlPath,
      `
actions:
- declaration:
    name: a_declaration`,
    );

    // Add an action with a test, reads from declaration
    fs.writeFileSync(
      action1SqlxPath,
      `
config {
  type: "table",
}
SELECT a,b,c FROM \${ref("a_declaration")}
    `,
    );
    fs.writeFileSync(
      action1TestSqlxPath,
      `
config {
  type: "test",
  dataset: "action1"
}
input "a_declaration" {
  SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d
}
`,
    );

    // Add an action with a test, reads from previous action
    fs.writeFileSync(
      action2SqlxPath,
      `
config {
  type: "table",
}
SELECT a,b FROM \${ref("action1")}
    `,
    );
    fs.writeFileSync(
      action2TestSqlxPath,
      `
config {
  type: "test",
  dataset: "action2"
}
input "action1" {
  SELECT 1 AS a, 2 AS b, 3 AS c
}
SELECT 1 AS a, 2 AS b`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
    expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
      `Expected query is empty.`,
    );
  });

  suite("incremental datasets", () => {
    test(`sqlx test uses the full-refresh query`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const definitionsDir = path.join(projectDir, "definitions");
      const actionsYamlPath = path.join(definitionsDir, "actions.yaml");
      const actionSqlxPath = path.join(definitionsDir, "action.sqlx");
      const actionTestSqlxPath = path.join(definitionsDir, "action_test.sqlx");

      fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(definitionsDir);
      fs.writeFileSync(
        actionsYamlPath,
        `
actions:
- declaration:
    name: source`,
      );
      fs.writeFileSync(
        actionSqlxPath,
        `
config {
  type: "incremental"
}
pre_operations {
  DECLARE max_ts TIMESTAMP DEFAULT (SELECT MAX(ts) FROM \${self()})
}
SELECT id, ts FROM \${ref("source")}
\${when(incremental(), \`WHERE ts > max_ts\`, \`WHERE ts IS NOT NULL\`)}
incremental_where {
  ts > TIMESTAMP("2000-01-01")
}
post_operations {
  SELECT "post_op"
}`,
      );
      fs.writeFileSync(
        actionTestSqlxPath,
        `
config {
  type: "test",
  dataset: "action"
}
input "source" {
  SELECT 1 AS id, TIMESTAMP("2026-01-01") AS ts
}
SELECT 1 AS id, TIMESTAMP("2026-01-01") AS ts`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      // The ref is replaced by the input and the false branch of when(incremental()) is used. The
      // pre-operation, incremental_where predicate and post-operation are not part of the query.
      expect(asPlainObject(result.compile.compiledGraph.tests)).deep.equals(
        asPlainObject([
          {
            name: "action_test",
            testQuery:
              '\n\n\nSELECT id, ts FROM (\n  SELECT 1 AS id, TIMESTAMP("2026-01-01") AS ts\n)\nWHERE ts IS NOT NULL\n\n',
            expectedOutputQuery: '\n\n\nSELECT 1 AS id, TIMESTAMP("2026-01-01") AS ts',
            fileName: "definitions/action_test.sqlx",
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action_test",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action_test",
            },
          },
        ]),
      );
    });

    test(`js api test uses the full-refresh query`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const definitionsDir = path.join(projectDir, "definitions");
      const fileJsPath = path.join(definitionsDir, "file.js");

      fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(definitionsDir);
      fs.writeFileSync(
        fileJsPath,
        `
declare({ name: "source" });
publish("action", { type: "incremental" }).query(
  (ctx) =>
    \`SELECT id FROM \${ctx.ref("source")} \${ctx.when(
      ctx.incremental(),
      \`WHERE id > (SELECT MAX(id) FROM \${ctx.self()})\`,
      "WHERE TRUE",
    )}\`,
);
test("action_test")
  .dataset("action")
  .input("source", "SELECT 1 AS id")
  .expect("SELECT 1 AS id");
`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tests)).deep.equals(
        asPlainObject([
          {
            name: "action_test",
            testQuery: "SELECT id FROM (SELECT 1 AS id) WHERE TRUE",
            expectedOutputQuery: "SELECT 1 AS id",
            fileName: "definitions/file.js",
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action_test",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action_test",
            },
          },
        ]),
      );
    });

    test(`refs in either argument of when() need an input`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const definitionsDir = path.join(projectDir, "definitions");
      const actionsYamlPath = path.join(definitionsDir, "actions.yaml");
      const actionSqlxPath = path.join(definitionsDir, "action.sqlx");
      const actionTestSqlxPath = path.join(definitionsDir, "action_test.sqlx");

      fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
      fs.mkdirSync(definitionsDir);
      fs.writeFileSync(
        actionsYamlPath,
        `
actions:
- declaration:
    name: source
- declaration:
    name: other`,
      );
      fs.writeFileSync(
        actionSqlxPath,
        `
config { type: "incremental" }
SELECT id FROM \${ref("source")}
\${when(incremental(), \`WHERE id IN (SELECT id FROM \${ref("other")})\`)}`,
      );
      fs.writeFileSync(
        actionTestSqlxPath,
        `
config { type: "test", dataset: "action" }
input "source" { SELECT 1 AS id }
SELECT 1 AS id`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      const errors = result.compile.compiledGraph.graphErrors.compilationErrors;
      expect(errors.length).equals(1);
      expect(errors[0].message).contains(
        `Input for dataset "{"name":"other"}" has not been provided.`,
      );
    });

    test(`includeTestsInCompiledGraph makes the incremental table depend on its test`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const definitionsDir = path.join(projectDir, "definitions");
      const actionSqlxPath = path.join(definitionsDir, "action.sqlx");
      const actionTestSqlxPath = path.join(definitionsDir, "action_test.sqlx");

      fs.writeFileSync(
        workflowSettingsPath,
        `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
includeTestsInCompiledGraph: true
`,
      );
      fs.mkdirSync(definitionsDir);
      fs.writeFileSync(
        actionSqlxPath,
        `
config { type: "incremental" }
SELECT 1 AS id`,
      );
      fs.writeFileSync(
        actionTestSqlxPath,
        `
config { type: "test", dataset: "action" }
SELECT 1 AS id`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            target: { database: "defaultProject", schema: "defaultDataset", name: "action" },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action",
            },
            dependencyTargets: [
              { database: "defaultProject", schema: "defaultDataset", name: "action_test" },
            ],
            disabled: false,
            enumType: "INCREMENTAL",
            fileName: "definitions/action.sqlx",
            hermeticity: "NON_HERMETIC",
            incrementalQuery: "\n\nSELECT 1 AS id",
            incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
            onSchemaChange: "IGNORE",
            protected: false,
            query: "\n\nSELECT 1 AS id",
            type: "incremental",
          },
        ]),
      );
    });
  });

  suite("datasets without an ahead-of-time query", () => {
    ["table", "view", "incremental"].forEach((datasetType) => {
      test(`test on a JiT-compiled ${datasetType} fails to compile with a clear error`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
        const definitionsDir = path.join(projectDir, "definitions");
        const actionSqlxPath = path.join(definitionsDir, "action.sqlx");
        const actionTestSqlxPath = path.join(definitionsDir, "action_test.sqlx");

        fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
        fs.mkdirSync(definitionsDir);
        fs.writeFileSync(
          actionSqlxPath,
          `config { type: "${datasetType}", compilation_mode: "jit" }
js {
  const foo = "bar";
}
SELECT 1 AS id`,
        );
        fs.writeFileSync(
          actionTestSqlxPath,
          `
config { type: "test", dataset: "action" }
SELECT 1 AS id`,
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        const errors = result.compile.compiledGraph.graphErrors.compilationErrors;
        expect(errors.map((error) => error.message)).deep.equals([
          `Dataset defaultProject.defaultDataset.action has no query to test. Datasets that use JiT compilation, or that define no query, can't be tested.`,
        ]);
      });
    });
    ["table", "incremental"].forEach((datasetType) => {
      test(`test on a js ${datasetType} with no query fails to compile with a clear error`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
        const definitionsDir = path.join(projectDir, "definitions");
        const fileJsPath = path.join(definitionsDir, "file.js");

        fs.writeFileSync(workflowSettingsPath, VALID_WORKFLOW_SETTINGS_YAML);
        fs.mkdirSync(definitionsDir);
        fs.writeFileSync(
          fileJsPath,
          `
publish("action", { type: "${datasetType}" });
test("action_test").dataset("action").expect("SELECT 1 AS id");
`,
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        const messages = result.compile.compiledGraph.graphErrors.compilationErrors.map(
          (error) => error.message,
        );
        expect(messages).to.include(
          `Dataset defaultProject.defaultDataset.action has no query to test. Datasets that use JiT compilation, or that define no query, can't be tested.`,
        );
      });
    });
  });

  test(`test with includeTestsInCompiledGraph set`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const definitionsDir = path.join(projectDir, "definitions");
    const actionsYamlPath = path.join(definitionsDir, "actions.yaml");
    const actionSqlPath = path.join(definitionsDir, "action.sql");
    const actionTestSqlxPath = path.join(definitionsDir, "action_test.sqlx");

    fs.writeFileSync(
      workflowSettingsPath,
      `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
includeTestsInCompiledGraph: true
`,
    );
    fs.mkdirSync(definitionsDir);
    fs.writeFileSync(
      actionsYamlPath,
      `
actions:
- table:
    filename: action.sql`,
    );
    fs.writeFileSync(actionSqlPath, "SELECT 1");
    fs.writeFileSync(
      actionTestSqlxPath,
      `
config {
  type: "test",
  dataset: "action",
  tags: ["tag1", "tag2"]
}
SELECT 1`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.tests)).deep.equals(
      asPlainObject([
        {
          // Original test properties
          name: "action_test",
          testQuery: "SELECT 1",
          expectedOutputQuery: "\n\nSELECT 1",
          fileName: "definitions/action_test.sqlx",
          tags: ["tag1", "tag2"],
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "action_test",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "action_test",
          },
        },
      ]),
    );
    expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
      asPlainObject([
        {
          target: {
            database: "defaultProject",
            name: "action",
            schema: "defaultDataset",
          },
          canonicalTarget: {
            database: "defaultProject",
            name: "action",
            schema: "defaultDataset",
          },
          disabled: false,
          enumType: "TABLE",
          fileName: "definitions/action.sql",
          hermeticity: "NON_HERMETIC",
          query: "SELECT 1",
          type: "table",
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action_test",
            },
          ],
        },
      ]),
    );
  });

  test(`test supports defaultJobLabels and jobLabels in sqlx and js`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const definitionsDir = path.join(projectDir, "definitions");

    fs.writeFileSync(
      workflowSettingsPath,
      `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
defaultJobLabels:
  env: dev
  team: test_team
`,
    );
    fs.mkdirSync(definitionsDir);
    fs.writeFileSync(
      path.join(definitionsDir, "actions.yaml"),
      `
actions:
- table:
    filename: action.sql`,
    );
    fs.writeFileSync(path.join(definitionsDir, "action.sql"), "SELECT 1");
    fs.writeFileSync(
      path.join(definitionsDir, "sqlx_test.sqlx"),
      `
config {
  type: "test",
  dataset: "action",
  jobLabels: {
    env: "prod",
    cost_center: "analytics"
  }
}
SELECT 1`,
    );
    // Tests that don't set jobLabels must still inherit defaultJobLabels.
    fs.writeFileSync(
      path.join(definitionsDir, "sqlx_defaults_test.sqlx"),
      `
config {
  type: "test",
  dataset: "action"
}
SELECT 1`,
    );
    fs.writeFileSync(
      path.join(definitionsDir, "js_test.js"),
      `
test("js_test")
  .dataset("action")
  .jobLabels({ env: "staging" })
  .expect("SELECT 1");

test("js_defaults_test")
  .dataset("action")
  .expect("SELECT 1");
`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const testByName = new Map(result.compile.compiledGraph.tests.map((t) => [t.name, t]));
    expect(asPlainObject(testByName.get("sqlx_test")?.actionDescriptor)).deep.equals({
      jobLabels: {
        env: "prod",
        team: "test_team",
        cost_center: "analytics",
      },
    });
    expect(asPlainObject(testByName.get("js_test")?.actionDescriptor)).deep.equals({
      jobLabels: {
        env: "staging",
        team: "test_team",
      },
    });
    expect(asPlainObject(testByName.get("sqlx_defaults_test")?.actionDescriptor)).deep.equals({
      jobLabels: {
        env: "dev",
        team: "test_team",
      },
    });
    expect(asPlainObject(testByName.get("js_defaults_test")?.actionDescriptor)).deep.equals({
      jobLabels: {
        env: "dev",
        team: "test_team",
      },
    });
  });
});
