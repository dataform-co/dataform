import { expect } from "chai";

import { dataform } from "df/protos/ts";
import {
  asPlainObject,
  suite,
  test,
  writeDefinitionFile,
  writeWorkflowSettingsFile
} from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML
} from "df/testing/run_core";

suite("assertion", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
    test(`assertions can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        // If change, then change test "sqlx config options checks for assertions".
        `
actions:
- assertion:
    name: name
    dataset: dataset
    project: project
    dependencyTargets:
      - name: operation
        dataset: defaultDataset
        project: defaultProject
    filename: action.sql
    tags:
      - tagA
      - tagB
    disabled: true,
    description: description
    hermetic: true
    dependOnDependencyAssertions: true
    reservation: reservation`
      );
      writeDefinitionFile(projectDir, "action.sql", "SELECT 1");
      writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "project",
              schema: "dataset",
              name: "name"
            },
            canonicalTarget: {
              database: "project",
              schema: "dataset",
              name: "name"
            },
            actionDescriptor: {
              description: "description",
              reservation: "reservation"
            },
            disabled: true,
            fileName: "definitions/action.sql",
            hermeticity: "HERMETIC",
            tags: ["tagA", "tagB"],
            query: "SELECT 1",
            dependencyTargets: [
              {
                name: "operation",
                schema: "defaultDataset",
                database: "defaultProject"
              }
            ]
          }
        ])
      );
    });

    test(`action config options`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");
      writeDefinitionFile(projectDir, "filename.sql", "SELECT 1");
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- assertion:
    name: name
    dataset: dataset
    project: project
    dependencyTargets:
    - name: operation
    filename: filename.sql
    tags:
    - tagA
    - tagB
    disabled: true
    description: description
    hermetic: true
    dependOnDependencyAssertions: true
`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "project",
              schema: "dataset",
              name: "name"
            },
            canonicalTarget: {
              database: "project",
              schema: "dataset",
              name: "name"
            },
            actionDescriptor: {
              description: "description"
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "operation"
              }
            ],
            disabled: true,
            fileName: "definitions/filename.sql",
            hermeticity: "HERMETIC",
            tags: ["tagA", "tagB"],
            query: "SELECT 1"
          }
        ])
      );
    });
  });

  suite("disableAssertions", () => {
    [
      {
        extension: "sqlx",
        setupFiles: (projectDir: string) => {
          writeDefinitionFile(
            projectDir,
            "assertion.sqlx",
            `config { type: "assertion" }SELECT 1 WHERE FALSE`
          );
        }
      },
      {
        extension: "js",
        setupFiles: (projectDir: string) => {
          writeDefinitionFile(
            projectDir,
            "assertion.js",
            `assert("assertion").query("SELECT 1 WHERE FALSE");`
          );
        }
      },
      {
        extension: "sql",
        setupFiles: (projectDir: string) => {
          writeDefinitionFile(
            projectDir,
            "actions.yaml",
            `
actions:
- assertion:
    name: assertion
    filename: assertion.sql`
          );
          writeDefinitionFile(projectDir, "assertion.sql", "SELECT 1 WHERE FALSE");
        }
      }
    ].forEach(testCase => {
      test(`disables ${testCase.extension} file assertions when disableAssertions is true`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);

        testCase.setupFiles(projectDir);

        const coreRequest = coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            disableAssertions: true
          })
        );
        const result = runMainInVm(coreRequest);

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
          asPlainObject([
            {
              canonicalTarget: {
                database: "defaultProject",
                name: "assertion",
                schema: "defaultDataset"
              },
              disabled: true,
              fileName: `definitions/assertion.${testCase.extension}`,
              query: "SELECT 1 WHERE FALSE",
              target: {
                database: "defaultProject",
                name: "assertion",
                schema: "defaultDataset"
              }
            }
          ])
        );
      });
    });

    ["table", "view", "incremental"].forEach(tableType => {
      test(`disables inline ${tableType} assertions when disableAssertions is true`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(
          projectDir,
          "test.sqlx",
          `config {
            type: "${tableType}",
            assertions: {
              uniqueKey: ["id"],
              nonNull: ["name"],
              rowConditions: ["id > 0"]
            }
          }
          SELECT 1 as id, 'test' as name`
        );

        const coreRequest = coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            disableAssertions: true
          })
        );
        const result = runMainInVm(coreRequest);

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
          asPlainObject([
            {
              canonicalTarget: {
                database: "defaultProject",
                name: "defaultDataset_test_assertions_uniqueKey_0",
                schema: "defaultDataset"
              },
              dependencyTargets: [
                {
                  database: "defaultProject",
                  name: "test",
                  schema: "defaultDataset"
                }
              ],
              disabled: true,
              fileName: "definitions/test.sqlx",
              parentAction: {
                database: "defaultProject",
                name: "test",
                schema: "defaultDataset"
              },
              query:
                "\nSELECT\n  *\nFROM (\n  SELECT\n    id,\n    COUNT(1) AS index_row_count\n  FROM `defaultProject.defaultDataset.test`\n  GROUP BY id\n  ) AS data\nWHERE index_row_count > 1\n",
              target: {
                database: "defaultProject",
                name: "defaultDataset_test_assertions_uniqueKey_0",
                schema: "defaultDataset"
              }
            },
            {
              canonicalTarget: {
                database: "defaultProject",
                name: "defaultDataset_test_assertions_rowConditions",
                schema: "defaultDataset"
              },
              dependencyTargets: [
                {
                  database: "defaultProject",
                  name: "test",
                  schema: "defaultDataset"
                }
              ],
              disabled: true,
              fileName: "definitions/test.sqlx",
              parentAction: {
                database: "defaultProject",
                name: "test",
                schema: "defaultDataset"
              },
              query:
                "\nSELECT\n  'id > 0' AS failing_row_condition,\n  *\nFROM `defaultProject.defaultDataset.test`\nWHERE NOT (id > 0)\nUNION ALL\nSELECT\n  'name IS NOT NULL' AS failing_row_condition,\n  *\nFROM `defaultProject.defaultDataset.test`\nWHERE NOT (name IS NOT NULL)\n",
              target: {
                database: "defaultProject",
                name: "defaultDataset_test_assertions_rowConditions",
                schema: "defaultDataset"
              }
            }
          ])
        );
        expect(result.compile.compiledGraph.tables.length).equals(1);
      });
    });
  });
});
