import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { version } from "df/core/version";
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
  VALID_DATAFORM_JSON,
  VALID_WORKFLOW_SETTINGS_YAML
} from "df/testing/run_core";

suite("workflow settings", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  test(`valid workflow_settings.yaml is present`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
      asPlainObject({
        warehouse: "bigquery",
        defaultDatabase: "defaultProject",
        defaultSchema: "defaultDataset",
        defaultLocation: "US"
      })
    );
  });

  // dataform.json for workflow settings is deprecated, but still currently supported.
  test(`a valid dataform.json is present`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
      asPlainObject({
        defaultDatabase: "defaultProject",
        defaultLocation: "US",
        defaultSchema: "defaultDataset"
      })
    );
  });

  test(`fails when no workflow settings file is present`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "Failed to resolve workflow_settings.yaml"
    );
  });

  test(`fails when both workflow settings and dataform.json files are present`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
    );
  });

  test(`fails when workflow_settings.yaml cannot be represented in JSON format`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, "&*19132sdS:asd:");

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "workflow_settings.yaml is invalid"
    );
  });

  test(`fails when workflow settings fails to be parsed`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
someKey: and an extra: colon
`
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "workflow_settings.yaml is not a valid YAML file: YAMLException: bad indentation"
    );
  });

  test(`fails when dataform.json is an invalid json file`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "dataform.json"), '{keyWithNoQuotes: "validValue"}');

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "Expected property name or '}' in JSON at position 1 (line 1 column 2)"
    );
  });

  test(`fails when a valid workflow_settings.yaml contains unknown fields`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, "notAProjectConfigField: value");

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      `Workflow settings error: Unexpected property "notAProjectConfigField", or property value type of "string" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-WorkflowSettings for allowed properties.`
    );
  });

  test(`fails when a valid workflow_settings.yaml base level is an array`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, "- someArrayEntry");

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "Expected a top-level object, but found an array"
    );
  });

  test(`fails when a valid dataform.json contains unknown fields`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "dataform.json"), `{"notAProjectConfigField": "value"}`);

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      `Dataform json error: Unexpected property "notAProjectConfigField", or property value type of "string" is incorrect.`
    );
  });

  test(`does not fail when defaultLocation is not present in workflow_settings.yaml`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
dataformCoreVersion: ${version}
defaultProject: project`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
      asPlainObject({
        warehouse: "bigquery",
        defaultDatabase: "project"
      })
    );
  });

  test(`workflow settings and project config overrides are merged and applied within SQLX files`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: defaultProject
defaultLocation: locationInWorkflowSettings
vars:
  selectVar: selectVal
`
    );
    writeDefinitionFile(
      projectDir,
      "file.sqlx",
      `
config {
  type: "table",
  database: dataform.projectConfig.vars.projectVar,
}
select 1 AS \${dataform.projectConfig.vars.selectVar}`
    );
    const coreExecutionRequest = dataform.CoreExecutionRequest.create({
      compile: {
        compileConfig: {
          projectDir,
          filePaths: ["definitions/file.sqlx"],
          projectConfigOverride: {
            defaultLocation: "locationInOverride",
            vars: {
              projectVar: "projectVal"
            }
          }
        }
      }
    });

    const result = runMainInVm(coreExecutionRequest);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
      asPlainObject({
        dataformCoreVersion: version,
        graphErrors: {},
        jitData: {},
        projectConfig: {
          defaultDatabase: "defaultProject",
          defaultLocation: "locationInOverride",
          vars: {
            projectVar: "projectVal",
            selectVar: "selectVal"
          },
          warehouse: "bigquery"
        },
        tables: [
          {
            canonicalTarget: {
              database: "projectVal",
              name: "file"
            },
            disabled: false,
            enumType: "TABLE",
            fileName: "definitions/file.sqlx",
            hermeticity: "NON_HERMETIC",
            query: "\n\nselect 1 AS selectVal",
            target: {
              database: "projectVal",
              name: "file"
            },
            type: "table"
          }
        ],
        targets: [
          {
            database: "projectVal",
            name: "file"
          }
        ]
      })
    );
  });

  suite("dataform core version", () => {
    test(`main fails when the workflow settings version is not the installed current version`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        `
dataformCoreVersion: 1.0.0
defaultProject: dataform`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        `Version mismatch: workflow settings specifies version 1.0.0, but ${version} was found`
      );
    });

    test(`main succeeds when workflow settings contains the matching version`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        `
dataformCoreVersion: ${version}
defaultProject: project
defaultLocation: US`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
        asPlainObject({
          warehouse: "bigquery",
          defaultDatabase: "project",
          defaultLocation: "US"
        })
      );
    });
  });

  suite("variables", () => {
    test(`variables in workflow_settings.yaml must be strings`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        `
vars:
  intValue: 1
  strValue: "str"`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "Custom variables defined in workflow settings can only be strings."
      );
    });

    test(`variables in dataform.json must be strings`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "dataform.json"),
        `{"vars": { "intVar": 1, "strVar": "str" } }`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "Custom variables defined in workflow settings can only be strings."
      );
    });

    test(`variables can be referenced in SQLX`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(
        projectDir,
        `
defaultLocation: "us"
vars:
  descriptionVar: descriptionValue
  columnVar: columnValue`
      );
      writeDefinitionFile(
        projectDir,
        "file.sqlx", // TODO(https://github.com/dataform-co/dataform/issues/1295): add a test and fix
        // functionality for assertions overriding database.
        `
config {
  type: "table",
  database: dataform.projectConfig.vars.databaseVar,
  schema: "tableSchema",
  description: dataform.projectConfig.vars.descriptionVar,
  assertions: {
    nonNull: [dataform.projectConfig.vars.columnVar],
  }
}
select 1 AS \${dataform.projectConfig.vars.columnVar}`
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: {
          compileConfig: {
            projectDir,
            filePaths: ["definitions/file.sqlx"],
            projectConfigOverride: {
              vars: {
                databaseVar: "databaseVal"
              }
            }
          }
        }
      });

      const result = runMainInVm(coreExecutionRequest);

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
        asPlainObject({
          assertions: [
            {
              canonicalTarget: {
                name: "tableSchema_file_assertions_rowConditions"
              },
              dependencyTargets: [
                {
                  database: "databaseVal",
                  name: "file",
                  schema: "tableSchema"
                }
              ],
              fileName: "definitions/file.sqlx",
              parentAction: {
                database: "databaseVal",
                name: "file",
                schema: "tableSchema"
              },
              query:
                "\nSELECT\n  'columnValue IS NOT NULL' AS failing_row_condition,\n  *\nFROM `databaseVal.tableSchema.file`\nWHERE NOT (columnValue IS NOT NULL)\n",
              target: {
                name: "tableSchema_file_assertions_rowConditions"
              }
            }
          ],
          dataformCoreVersion: version,
          graphErrors: {},
          jitData: {},
          projectConfig: {
            defaultLocation: "us",
            vars: {
              databaseVar: "databaseVal",
              descriptionVar: "descriptionValue",
              columnVar: "columnValue"
            },
            warehouse: "bigquery"
          },
          tables: [
            {
              actionDescriptor: {
                description: "descriptionValue"
              },
              canonicalTarget: {
                database: "databaseVal",
                name: "file",
                schema: "tableSchema"
              },
              disabled: false,
              enumType: "TABLE",
              fileName: "definitions/file.sqlx",
              query: "\n\nselect 1 AS columnValue",
              target: {
                database: "databaseVal",
                name: "file",
                schema: "tableSchema"
              },
              type: "table",
              hermeticity: "NON_HERMETIC"
            }
          ],
          targets: [
            {
              name: "tableSchema_file_assertions_rowConditions"
            },
            {
              database: "databaseVal",
              name: "file",
              schema: "tableSchema"
            }
          ]
        })
      );
    });
  });
});
