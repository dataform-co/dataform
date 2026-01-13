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
  dataset: "action",
  tags: ["tag1", "tag2"]
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
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action_test"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action_test"
            },
            tags: ["tag1", "tag2"],
          }
        ])
      );
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
             "target": {
              "database": "defaultProject",
              "name": "action",
              "schema": "defaultDataset"
            },
            "canonicalTarget": {
              "database": "defaultProject",
              "name": "action",
              "schema": "defaultDataset"
            },
            "disabled": false,
            "enumType": "TABLE",
            "fileName": "definitions/action.sql",
            "hermeticity": "NON_HERMETIC",
            "query": "SELECT 1",
            "type": "table"
          }
      ]));
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
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action1_test"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action1_test"
            },
          },
          {
            // Original test properties
            name: "action2_test",
            testQuery: "\n\nSELECT a,b FROM (\n  SELECT 1 AS a, 2 AS b, 3 AS c\n)\n    ",
            expectedOutputQuery: "\n\n\nSELECT 1 AS a, 2 AS b",
            fileName: "definitions/action2_test.sqlx",
            
            // New properties
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action2_test"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action2_test"
            },
          }
        ])
      );
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
             "target": {
              "database": "defaultProject",
              "name": "action1",
              "schema": "defaultDataset"
            },
            "canonicalTarget": {
              "database": "defaultProject",
              "name": "action1",
              "schema": "defaultDataset"
            },
            "dependencyTargets": [
              {
                "database": "defaultProject",
                "name": "a_declaration",
                "schema": "defaultDataset"
              }
            ],
            "disabled": false,
            "enumType": "TABLE",
            "fileName": "definitions/action1.sqlx",
            "hermeticity": "NON_HERMETIC",
            "query": "\n\nSELECT a,b,c FROM `defaultProject.defaultDataset.a_declaration`\n    ",
            "type": "table"
          },
          {
            "target": {
              "database": "defaultProject",
              "name": "action2",
              "schema": "defaultDataset"
            },
            "canonicalTarget": {
              "database": "defaultProject",
              "name": "action2",
              "schema": "defaultDataset"
            },
            "dependencyTargets": [
              {
                "database": "defaultProject",
                "name": "action1",
                "schema": "defaultDataset"
              }
            ],
            "disabled": false,
            "enumType": "TABLE",
            "fileName": "definitions/action2.sqlx",
            "hermeticity": "NON_HERMETIC",
            "query": "\n\nSELECT a,b FROM `defaultProject.defaultDataset.action1`\n    ",
            "type": "table"
          }
        ])
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
      fs.writeFileSync(schema1actionSqlxPath, `
config {
  schema: "schema1",
  name: "action",
  type: "table",
}
SELECT 1
    `);
      fs.writeFileSync(schema2actionSqlxPath, `
config {
  schema: "schema2",
  name: "action",
  type: "table",
}
SELECT 2
    `);
      fs.writeFileSync(schema1actionTestSqlxPath, `
config {
  type: "test",
  dataset: {
    schema: "schema1",
    name: "action",
  },
}
SELECT 1`);
      fs.writeFileSync(schema2actionTestSqlxPath, `
config {
  type: "test",
  dataset: {
    schema: "schema2",
    name: "action",
  },
}
SELECT 2`);

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
              name: "schema1_action_test"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "schema1",
              name: "schema1_action_test"
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
              name: "schema2_action_test"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "schema2",
              name: "schema2_action_test"
            },
          },
        ])
      );
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
             "target": {
              "database": "defaultProject",
              "name": "action",
              "schema": "schema1"
            },
            "canonicalTarget": {
              "database": "defaultProject",
              "name": "action",
              "schema": "schema1"
            },
            "disabled": false,
            "enumType": "TABLE",
            "fileName": "definitions/schema1_action.sqlx",
            "hermeticity": "NON_HERMETIC",
            "query": "\n\nSELECT 1\n    ",
            "type": "table"
          },
          {
             "target": {
              "database": "defaultProject",
              "name": "action",
              "schema": "schema2"
            },
            "canonicalTarget": {
              "database": "defaultProject",
              "name": "action",
              "schema": "schema2"
            },
            "disabled": false,
            "enumType": "TABLE",
            "fileName": "definitions/schema2_action.sqlx",
            "hermeticity": "NON_HERMETIC",
            "query": "\n\nSELECT 2\n    ",
            "type": "table"
          }
      ]));
    });
});

