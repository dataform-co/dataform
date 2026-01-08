// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML
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
      fs.writeFileSync(actionsYamlPath, `
actions:
- table:
    filename: action.sql`
      );
      fs.writeFileSync(actionSqlPath, "SELECT 1");
      fs.writeFileSync(actionTestSqlxPath, `
config {
  type: "test",
  dataset: "action"
}
SELECT 1`);

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

            // New properties
            testTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            query: "SELECT 1",
            resolveSchema: false,
          }
        ])
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
      fs.writeFileSync(actionsYamlPath, `
actions:
- declaration:
    name: a_declaration`
      );

      // Add an action with a test, reads from declaration
      fs.writeFileSync(action1SqlxPath, `
config {
  type: "table",
}
SELECT a,b,c FROM \${ref("a_declaration")}
    `);
      fs.writeFileSync(action1TestSqlxPath, `
config {
  type: "test",
  dataset: "action1"
}
input "a_declaration" {
  SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d
}
SELECT 1 AS a, 2 AS b, 3 AS c`);


      // Add an action with a test, reads from previous action
      fs.writeFileSync(action2SqlxPath, `
config {
  type: "table",
}
SELECT a,b FROM \${ref("action1")}
    `);
      fs.writeFileSync(action2TestSqlxPath, `
config {
  type: "test",
  dataset: "action2"
}
input "action1" {
  SELECT 1 AS a, 2 AS b, 3 AS c
}
SELECT 1 AS a, 2 AS b`);

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
            
            // New properties
            testTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action1"
            },
            inputs: [{
              query: "\n  SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d\n",
              target: {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "a_declaration"
              }
            }],
            query: "\n\nSELECT a,b,c FROM `defaultProject.defaultDataset.a_declaration`\n    ",
            resolveSchema: false,
          },
          {
            // Original test properties
            name: "action2_test",
            testQuery: "\n\nSELECT a,b FROM (\n  SELECT 1 AS a, 2 AS b, 3 AS c\n)\n    ",
            expectedOutputQuery: "\n\n\nSELECT 1 AS a, 2 AS b",
            fileName: "definitions/action2_test.sqlx",
            
            // New properties
            testTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action2"
            },
            inputs: [{
              query: "\n  SELECT 1 AS a, 2 AS b, 3 AS c\n",
              target: {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "action1"
              }
            }],
            query: "\n\nSELECT a,b FROM `defaultProject.defaultDataset.action1`\n    ",
            resolveSchema: false,
          }
        ])
      );
    });
});
