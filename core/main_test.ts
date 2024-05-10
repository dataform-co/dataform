// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml } from "js-yaml";
import * as path from "path";
import { CompilerFunction, NodeVM } from "vm2";

import { decode64, encode64 } from "df/common/protos";
import { compile } from "df/core/compilers";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

const SOURCE_EXTENSIONS = ["js", "sql", "sqlx", "yaml", "ipynb"];

const VALID_WORKFLOW_SETTINGS_YAML = `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
`;

const VALID_DATAFORM_JSON = `
{
  "defaultDatabase": "defaultProject",
  "defaultSchema": "defaultDataset",
  "defaultLocation": "US"
}
`;

class TestConfigs {
  public static bigquery = dataform.WorkflowSettings.create({
    defaultDataset: "defaultDataset",
    defaultLocation: "US"
  });

  public static bigqueryWithDefaultProject = dataform.WorkflowSettings.create({
    ...TestConfigs.bigquery,
    defaultProject: "defaultProject"
  });

  public static bigqueryWithDatasetSuffix = dataform.WorkflowSettings.create({
    ...TestConfigs.bigquery,
    datasetSuffix: "suffix"
  });

  public static bigqueryWithDefaultProjectAndDataset = dataform.WorkflowSettings.create({
    ...TestConfigs.bigqueryWithDefaultProject,
    projectSuffix: "suffix"
  });

  public static bigqueryWithNamePrefix = dataform.WorkflowSettings.create({
    ...TestConfigs.bigquery,
    namePrefix: "prefix"
  });
}

const EMPTY_NOTEBOOK_CONTENTS = '{ "cells": [] }';

suite("@dataform/core", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("session", () => {
    suite("resolve succeeds", () => {
      [
        TestConfigs.bigquery,
        TestConfigs.bigqueryWithDatasetSuffix,
        TestConfigs.bigqueryWithNamePrefix
      ].forEach(testConfig => {
        test(`resolve with name prefix "${testConfig.namePrefix}" and dataset suffix "${testConfig.datasetSuffix}"`, () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          fs.writeFileSync(
            path.join(projectDir, "workflow_settings.yaml"),
            dumpYaml(dataform.WorkflowSettings.create(testConfig))
          );
          fs.mkdirSync(path.join(projectDir, "definitions"));
          fs.writeFileSync(path.join(projectDir, "definitions/e.sqlx"), `config {type: "view"}`);
          fs.writeFileSync(path.join(projectDir, "definitions/file.sqlx"), "${resolve('e')}");

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          const suffix = testConfig.datasetSuffix ? `_${testConfig.datasetSuffix}` : "";
          const prefix = testConfig.namePrefix ? `${testConfig.namePrefix}_` : "";
          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(result.compile.compiledGraph.operations[0].queries[0]).deep.equals(
            `\`defaultDataset${suffix}.${prefix}e\``
          );
        });
      });
    });

    test("resolve fails", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, "definitions/file.sqlx"), "${resolve('e')}");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(asPlainObject(result.compile.compiledGraph.operations[0].queries[0])).deep.equals(``);
      expect(
        asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors[0].message)
      ).deep.equals(`Could not resolve "e"`);
    });

    suite("context methods", () => {
      [
        TestConfigs.bigqueryWithDefaultProjectAndDataset,
        { ...TestConfigs.bigqueryWithDatasetSuffix, defaultProject: "defaultProject" },
        { ...TestConfigs.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
      ].forEach(testConfig => {
        test(
          `assertions target context functions with project suffix '${testConfig.projectSuffix}', ` +
            `dataset suffix '${testConfig.datasetSuffix}', and name prefix '${testConfig.namePrefix}'`,
          () => {
            const projectDir = tmpDirFixture.createNewTmpDir();
            fs.writeFileSync(
              path.join(projectDir, "workflow_settings.yaml"),
              dumpYaml(dataform.WorkflowSettings.create(testConfig))
            );
            fs.mkdirSync(path.join(projectDir, "definitions"));
            fs.writeFileSync(
              path.join(projectDir, "definitions/file.js"),
              'assert("name", ctx => `${ctx.database()}.${ctx.schema()}.${ctx.name()}`)'
            );

            const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

            expect(
              asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors)
            ).deep.equals([]);
            expect(asPlainObject(result.compile.compiledGraph.assertions[0].query)).deep.equals(
              `defaultProject${testConfig.projectSuffix ? `_suffix` : ""}.` +
                `defaultDataset${testConfig.datasetSuffix ? `_suffix` : ""}.` +
                `${testConfig.namePrefix ? `prefix_` : ""}name`
            );
          }
        );
      });

      test("assertions database function fails when database is undefined on the proto", () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          dumpYaml(dataform.WorkflowSettings.create(TestConfigs.bigquery))
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, "definitions/file.js"),
          'assert("name", ctx => `${ctx.database()}.${ctx.schema()}.${ctx.name()}`)'
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(
          asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors?.[0]?.message)
        ).deep.equals("Warehouse does not support multiple databases");
      });
    });

    suite("filenames with multiple dots cause compilation errors", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, "definitions/table1.extradot.sqlx"), "SELECT 1");
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- operation:
    dataset: "dataset.extradot"
    filename: table2.extradot.sql`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/table2.extradot.sql"), "SELECT 2");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors
          .map(({ message }) => message)
          .sort()
      ).deep.equals([
        `Action target datasets cannot include '.'`,
        `Action target names cannot include '.'`,
        `Action target names cannot include '.'`
      ]);
    });
  });

  suite("actions", () => {
    test("disabled", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/table.sqlx"),
        "config { type: 'table', disabled: true }"
      );
      fs.writeFileSync(
        path.join(projectDir, "definitions/operation.sqlx"),
        "config { type: 'operations', disabled: false }"
      );
      fs.writeFileSync(
        path.join(projectDir, "definitions/assertion.sqlx"),
        "config { type: 'assertion', disabled: true }"
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables[0].disabled)).equals(true);
      expect(asPlainObject(result.compile.compiledGraph.operations[0].disabled)).equals(false);
      expect(asPlainObject(result.compile.compiledGraph.assertions[0].disabled)).equals(true);
    });
  });

  suite("sqlx special characters", () => {
    test("extract blocks", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        dumpYaml(dataform.WorkflowSettings.create(TestConfigs.bigquery))
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
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
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        dumpYaml(dataform.WorkflowSettings.create(TestConfigs.bigquery))
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      const fileContents = `select
  "\`",
  """\`"",
from \`location\``;
      fs.writeFileSync(path.join(projectDir, "definitions/file.sqlx"), fileContents);

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(result.compile.compiledGraph.operations[0].queries[0]).equals(fileContents);
    });

    test("backslashes appear to users as written", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        dumpYaml(dataform.WorkflowSettings.create(TestConfigs.bigquery))
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      const sqlContents = `select
  regexp_extract('01a_data_engine', '^(\\d{2}\\w)'),
  regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)'),
  regexp_extract('\\\\', ''),
  regexp_extract("", r"[0-9]\\"*"),
  """\\ \\? \\\\"""`;
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        `config { type: "table" }` + sqlContents + `pre_operations { ${sqlContents} }`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(result.compile.compiledGraph.tables[0].query.trim()).equals(sqlContents);
      expect(result.compile.compiledGraph.tables[0].preOps[0].trim()).equals(sqlContents);
    });

    test("strings appear to users as written", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        dumpYaml(dataform.WorkflowSettings.create(TestConfigs.bigquery))
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
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
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
        `config { type: "table" }` + sqlContents + `post_operations { ${sqlContents} }`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(result.compile.compiledGraph.tables[0].query.trim()).equals(sqlContents);
      expect(result.compile.compiledGraph.tables[0].postOps[0].trim()).equals(sqlContents);
    });
  });

  suite("workflow settings", () => {
    test(`main succeeds when a valid workflow_settings.yaml is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );

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
    test(`main succeeds when a valid dataform.json is present`, () => {
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

    test(`main fails when no workflow settings file is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "Failed to resolve workflow_settings.yaml"
      );
    });

    test(`main fails when both workflow settings and dataform.json files are present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
      );
    });

    test(`main fails when workflow_settings.yaml is an invalid yaml file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "&*19132sdS:asd:");

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "workflow_settings.yaml is invalid"
      );
    });

    test(`main fails when dataform.json is an invalid json file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "dataform.json"), '{keyWithNoQuotes: "validValue"}');

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "Unexpected token k in JSON at position 1"
      );
    });

    test(`main fails when a valid workflow_settings.yaml contains unknown fields`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        "notAProjectConfigField: value"
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        `Workflow settings error: Unexpected property "notAProjectConfigField", or property value type of "string" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-WorkflowSettings for allowed properties.`
      );
    });

    test(`main fails when a valid workflow_settings.yaml base level is an array`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "- someArrayEntry");

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "Expected a top-level object, but found an array"
      );
    });

    test(`main fails when a valid dataform.json contains unknown fields`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "dataform.json"),
        `{"notAProjectConfigField": "value"}`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        `Dataform json error: Unexpected property "notAProjectConfigField", or property value type of "string" is incorrect.`
      );
    });

    test(`workflow settings and project config overrides are merged and applied within SQLX files`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `
defaultProject: defaultProject
defaultLocation: locationInWorkflowSettings
vars:
  selectVar: selectVal
`
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.sqlx"),
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
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
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
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
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
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
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
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          `
defaultLocation: "us"
vars:
  descriptionVar: descriptionValue
  columnVar: columnValue`
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, "definitions/file.sqlx"),
          // TODO(https://github.com/dataform-co/dataform/issues/1295): add a test and fix
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
                type: "table"
              }
            ],
            targets: [
              {
                database: "databaseVal",
                name: "file",
                schema: "tableSchema"
              },
              {
                name: "tableSchema_file_assertions_rowConditions"
              }
            ]
          })
        );
      });
    });
  });

  suite("notebooks", () => {
    const createSimpleNotebookProject = (
      workflowSettingsYaml = VALID_WORKFLOW_SETTINGS_YAML
    ): string => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), workflowSettingsYaml);
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- notebook:
    filename: notebook.ipynb`
      );
      return projectDir;
    };

    test(`notebooks can be loaded via an actions config file`, () => {
      const projectDir = createSimpleNotebookProject();
      fs.writeFileSync(
        path.join(projectDir, "definitions/notebook.ipynb"),
        EMPTY_NOTEBOOK_CONTENTS
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.notebooks)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "notebook"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "notebook"
            },
            fileName: "definitions/notebook.ipynb",
            notebookContents: JSON.stringify({ cells: [] })
          }
        ])
      );
    });

    test(`notebook cell output is removed`, () => {
      const projectDir = createSimpleNotebookProject();
      fs.writeFileSync(
        path.join(projectDir, "definitions/notebook.ipynb"),
        JSON.stringify({
          cells: [
            { cell_type: "markdown", source: ["# Some title"], outputs: ["something"] },
            { cell_type: "code", source: ["print('hi')"], outputs: ["hi"] },
            { cell_type: "raw", source: ["print('hi')"] }
          ]
        })
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.notebooks)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "notebook"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "notebook"
            },
            fileName: "definitions/notebook.ipynb",
            notebookContents: JSON.stringify({
              cells: [
                { cell_type: "markdown", source: ["# Some title"], outputs: [] },
                { cell_type: "code", source: ["print('hi')"], outputs: [] },
                { cell_type: "raw", source: ["print('hi')"] }
              ]
            })
          }
        ])
      );
    });

    test(`notebook default runtime options are loaded`, () => {
      const projectDir = createSimpleNotebookProject(`
defaultProject: dataform
defaultLocation: US
defaultNotebookRuntimeOptions:
  outputBucket: gs://some-bucket`);
      fs.writeFileSync(
        path.join(projectDir, "definitions/notebook.ipynb"),
        EMPTY_NOTEBOOK_CONTENTS
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals({
        defaultDatabase: "dataform",
        defaultLocation: "US",
        defaultNotebookRuntimeOptions: {
          outputBucket: "gs://some-bucket"
        },
        warehouse: "bigquery"
      });
    });
  });

  suite("action configs", () => {
    test(`operations can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- operation:
    filename: action.sql`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/action.sql"), "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            fileName: "definitions/action.sql",
            queries: ["SELECT 1"]
          }
        ])
      );
    });

    test(`declarations can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- declaration:
    name: action`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.declarations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            }
          }
        ])
      );
    });

    test(`fails when filename is defined for declaration`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- declaration:
    fileName: doesnotexist.sql
    name: name`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        `Unexpected property "fileName", or property value type of "string" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-ActionConfigs for allowed properties.`
      );
    });

    test(`fails when target name is not defined for declaration`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- declaration:
    dataset: test`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "Declarations must have a populated 'name' field."
      );
    });

    test(`tables can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- table:
    filename: action.sql`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/action.sql"), "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            fileName: "definitions/action.sql",
            query: "SELECT 1",
            type: "table",
            enumType: "TABLE",
            disabled: false
          }
        ])
      );
    });

    test(`incremental tables can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- incrementalTable:
    filename: action.sql
    protected: true
    uniqueKey:
    -  someKey1
    -  someKey2`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/action.sql"), "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            fileName: "definitions/action.sql",
            query: "SELECT 1",
            incrementalQuery: "SELECT 1",
            type: "incremental",
            enumType: "INCREMENTAL",
            protected: true,
            disabled: false,
            uniqueKey: ["someKey1", "someKey2"]
          }
        ])
      );
    });

    test(`views can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- view:
    filename: action.sql`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/action.sql"), "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "action"
            },
            fileName: "definitions/action.sql",
            query: "SELECT 1",
            type: "view",
            enumType: "VIEW",
            disabled: false
          }
        ])
      );
    });

    test(`assertions can be loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- assertion:
    filename: action.sql`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/action.sql"), "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              name: "action"
            },
            canonicalTarget: {
              database: "defaultProject",
              name: "action"
            },
            fileName: "definitions/action.sql",
            query: "SELECT 1"
          }
        ])
      );
    });

    test(`fails when file is not found`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- operation:
    filename: doesnotexist.sql`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "Cannot find module 'definitions/doesnotexist.sql'"
      );
    });

    test(`fails when properties belonging to other action config types are populated for an action config`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- table:
    filename: action.sql
    materialized: true`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        `Unexpected property "materialized", or property value type of "boolean" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-ActionConfigs for allowed properties.`
      );
    });

    test(`filenames with non-UTF8 characters are valid`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- operation:
    filename: utf8characters:私🙂 and some spaces.sql`
      );
      fs.writeFileSync(
        path.join(projectDir, "definitions/utf8characters:私🙂 and some spaces.sql"),
        "SELECT 1"
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "utf8characters:私🙂 and some spaces"
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "utf8characters:私🙂 and some spaces"
            },
            fileName: "definitions/utf8characters:私🙂 and some spaces.sql",
            queries: ["SELECT 1"]
          }
        ])
      );
    });

    test(`dependency targets are loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- declaration:
    name: declaration
- table:
    filename: table.sql
    dependencyTargets:
    - name: declaration
- incrementalTable:
    filename: incrementalTable.sql
    dependencyTargets:
    - name: table
- view:
    filename: view.sql
    dependencyTargets:
    - name: incrementalTable
- operation:
    filename: operation.sql
    dependencyTargets:
    - name: view
- notebook:
    filename: notebook.ipynb
    dependencyTargets:
    - name: view`
      );
      ["table.sql", "incrementalTable.sql", "view.sql", "operation.sql"].forEach(filename => {
        fs.writeFileSync(path.join(projectDir, `definitions/${filename}`), "SELECT 1");
      });
      fs.writeFileSync(
        path.join(projectDir, `definitions/notebook.ipynb`),
        EMPTY_NOTEBOOK_CONTENTS
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    });

    test(`files can be loaded from the root directory and are normalized in the compiled graph`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- notebook:
    filename: ../contents.ipynb
- operation:
    filename: ../table.sql`
      );
      fs.writeFileSync(path.join(projectDir, `contents.ipynb`), JSON.stringify({ cells: [] }));
      fs.writeFileSync(path.join(projectDir, `table.sql`), "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(result.compile.compiledGraph.notebooks[0].fileName).deep.equals("contents.ipynb");
      expect(result.compile.compiledGraph.operations[0].fileName).deep.equals("table.sql");
    });
  });

  suite("sqlx config options checks for", () => {
    const exampleActionDescriptor = {
      inputSqlxConfigBlock: `
  columns: {
    column1Key: "column1Val",
    column2Key: {
      description: "description",
      columns: {
        nestedColumnKey: "nestedColumnVal"
      },
      displayName: "displayName",
      tags: ["tag3", "tag4"],
      bigqueryPolicyTags: ["bigqueryPolicyTag1", "bigqueryPolicyTag2"],
    }
  },`,
      outputActionDescriptor: {
        columns: [
          {
            description: "column1Val",
            path: ["column1Key"]
          },
          {
            bigqueryPolicyTags: ["bigqueryPolicyTag1", "bigqueryPolicyTag2"],
            description: "description",
            displayName: "displayName",
            path: ["column2Key"],
            tags: ["tag3", "tag4"]
          },
          {
            description: "nestedColumnVal",
            path: ["column2Key", "nestedColumnKey"]
          }
        ],
        description: "description"
      }
    };

    test(`assertions`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
      fs.writeFileSync(
        path.join(projectDir, "definitions/assertion.sqlx"),
        `
config {
  type: "assertion",
  name: "name",
  schema: "dataset",
  database: "project",
  dependencies: ["operation"],
  tags: ["tagA", "tagB"],
  disabled: true,
  description: "description",
  hermetic: true,
  dependOnDependencyAssertions: true,
}
SELECT 1`
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
            fileName: "definitions/assertion.sqlx",
            hermeticity: "HERMETIC",
            tags: ["tagA", "tagB"],
            query: "\n\nSELECT 1"
          }
        ])
      );
    });

    test(`declarations`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/assertion.sqlx"),
        `
config {
  type: "declaration",
  name: "name",
  schema: "dataset",
  database: "project",
  description: "description",
${exampleActionDescriptor.inputSqlxConfigBlock}
}`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.declarations)).deep.equals(
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
            fileName: "definitions/assertion.sqlx",
            actionDescriptor: exampleActionDescriptor.outputActionDescriptor
          }
        ])
      );
    });

    test("tables", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
      fs.writeFileSync(
        path.join(projectDir, "definitions/incremental_table.sqlx"),
        // Incremental table is the table type used here because it's the most complex.
        `
config {
  type: "incremental",
  disabled: true,
  protected: false,
  name: "name",
  bigquery: {
    partitionBy: "partitionBy",
    clusterBy: ["clusterBy"],
    updatePartitionFilter: "updatePartitionFilter",
    labels: {"key": "val"},
    partitionExpirationDays: 1,
    requirePartitionFilter: true,
    additionalOptions: {
      option1Key: "option1",
      option2Key: "option2",
    }
  },
  tags: ["tag1", "tag2"],
  uniqueKey: ["key1", "key2"],
  dependencies: ["operation"],
  hermetic: true,
  schema: "dataset",
  assertions: {
    uniqueKeys: [["uniqueKey1", "uniqueKey2"]],
    nonNull: "nonNull",
    rowConditions: ["rowConditions1", "rowConditions2"],
  },
  database: "project",
${exampleActionDescriptor.inputSqlxConfigBlock}
  description: "description",
  materialized: false
}
SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
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
          type: "incremental",
          disabled: true,
          // TODO(ekrekr): finish fixing this in https://github.com/dataform-co/dataform/pull/1718.
          // protected: false,
          hermeticity: "HERMETIC",
          bigquery: {
            additionalOptions: {
              option1Key: "option1",
              option2Key: "option2"
            },
            clusterBy: ["clusterBy"],
            labels: {
              key: "val"
            },
            partitionBy: "partitionBy",
            partitionExpirationDays: 1,
            requirePartitionFilter: true,
            updatePartitionFilter: "updatePartitionFilter"
          },
          tags: ["tag1", "tag2"],
          uniqueKey: ["key1", "key2"],
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "operation"
            }
          ],
          enumType: "INCREMENTAL",
          fileName: "definitions/incremental_table.sqlx",
          query: "\n\nSELECT 1",
          incrementalQuery: "\n\nSELECT 1",
          actionDescriptor: {
            ...exampleActionDescriptor.outputActionDescriptor,
            // sqlxConfig.bigquery.labels are placed as bigqueryLabels.
            bigqueryLabels: {
              key: "val"
            }
          }
        }
      ]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "dataset_name_assertions_uniqueKey_0"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "dataset_name_assertions_uniqueKey_0"
          },
          dependencyTargets: [
            {
              database: "project",
              schema: "dataset",
              name: "name"
            }
          ],
          disabled: true,
          fileName: "definitions/incremental_table.sqlx",
          parentAction: {
            database: "project",
            schema: "dataset",
            name: "name"
          },
          query:
            "\nSELECT\n  *\nFROM (\n  SELECT\n    uniqueKey1, uniqueKey2,\n    COUNT(1) AS index_row_count\n  FROM `project.dataset.name`\n  GROUP BY uniqueKey1, uniqueKey2\n  ) AS data\nWHERE index_row_count > 1\n",
          tags: ["tag1", "tag2"]
        },
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "dataset_name_assertions_rowConditions"
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "dataset_name_assertions_rowConditions"
          },
          dependencyTargets: [
            {
              database: "project",
              schema: "dataset",
              name: "name"
            }
          ],
          disabled: true,
          fileName: "definitions/incremental_table.sqlx",
          parentAction: {
            database: "project",
            schema: "dataset",
            name: "name"
          },
          query:
            "\nSELECT\n  'rowConditions1' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (rowConditions1)\nUNION ALL\nSELECT\n  'rowConditions2' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (rowConditions2)\nUNION ALL\nSELECT\n  'nonNull IS NOT NULL' AS failing_row_condition,\n  *\nFROM `project.dataset.name`\nWHERE NOT (nonNull IS NOT NULL)\n",
          tags: ["tag1", "tag2"]
        }
      ]);
    });

    test(`operations`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/table.sqlx"),
        `config {type: "view"} SELECT 1`
      );
      fs.writeFileSync(
        path.join(projectDir, "definitions/operation.sqlx"),
        `
config {
  type: "operations",
  name: "name",
  schema: "dataset",
  database: "project",
  dependencies: ["table"],
  tags: ["tagA", "tagB"],
  disabled: true,
  description: "description",
  hermetic: true,
  hasOutput: true,
  dependOnDependencyAssertions: true,
${exampleActionDescriptor.inputSqlxConfigBlock}
}
SELECT 1`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
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
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "table"
              }
            ],
            disabled: true,
            fileName: "definitions/operation.sqlx",
            hermeticity: "HERMETIC",
            hasOutput: true,
            tags: ["tagA", "tagB"],
            queries: ["\n\nSELECT 1"],
            actionDescriptor: exampleActionDescriptor.outputActionDescriptor
          }
        ])
      );
    });
  });

  suite("Assertions as dependencies", ({ beforeEach }) => {
    [
      TestConfigs.bigquery,
      TestConfigs.bigqueryWithDatasetSuffix,
      TestConfigs.bigqueryWithNamePrefix
    ].forEach(testConfig => {
      let projectDir: any;
      beforeEach("Create temporary dir and files", () => {
        projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          dumpYaml(dataform.WorkflowSettings.create(testConfig))
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, "definitions/A.sqlx"),
          `
config {
  type: "table",
  assertions: {rowConditions: ["test > 1"]}}
  SELECT 1 as test`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/A_assert.sqlx"),
          `
config {
  type: "assertion",
}
select test from \${ref("A")} where test > 3`
        );
        fs.writeFileSync(path.join(projectDir, "definitions/B.sql"), "SELECT 1");
        fs.writeFileSync(path.join(projectDir, "definitions/C.sql"), "SELECT 1");
        fs.writeFileSync(
          path.join(projectDir, `definitions/notebook.ipynb`),
          EMPTY_NOTEBOOK_CONTENTS
        );
      });

      test("When dependOnDependencyAssertions property is set to true, assertions from A are added as dependencies", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependOnDependencyAssertions: true,
  dependencies: ["A"]
}
select 1 as btest
`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(3);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_A_assertions_rowConditions"),
          prefixAdjustedName(testConfig.namePrefix, "A_assert")
        ]);
      });

      test("Setting includeDependentAssertions to true in config.dependencies adds assertions from that dependency to dependencyTargets", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependencies: [{name: "A", includeDependentAssertions: true}, "C"]
}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
  assertions: {
    rowConditions: ["test > 1"]
  }
}
SELECT 1 as test`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(4);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_A_assertions_rowConditions"),
          prefixAdjustedName(testConfig.namePrefix, "A_assert"),
          prefixAdjustedName(testConfig.namePrefix, "C")
        ]);
      });

      test("Setting includeDependentAssertions to true in ref, adds assertions from that dependency to dependencyTargets", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: true})}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
    assertions: {
      rowConditions: ["test > 1"]
  }
}
SELECT 1 as test`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(3);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "C"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_C_assertions_rowConditions")
        ]);
      });

      test("When dependOnDependencyAssertions=true and includeDependentAssertions=false, the assertions related to dependency should not be added to dependencyTargets", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependOnDependencyAssertions: true,
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: false})}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
    assertions: {
      rowConditions: ["test > 1"]
  }
}
SELECT 1 as test`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(4);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_A_assertions_rowConditions"),
          prefixAdjustedName(testConfig.namePrefix, "A_assert"),
          prefixAdjustedName(testConfig.namePrefix, "C")
        ]);
      });

      test("When dependOnDependencyAssertions=false and includeDependentAssertions=true, the assertions related to dependency should be added to dependencyTargets", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "operations",
  dependOnDependencyAssertions: false,
  dependencies: ["A"]
}
select * from \${ref({name: "C", includeDependentAssertions: true})}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
    assertions: {
      rowConditions: ["test > 1"]
  }
}
SELECT 1 as test`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.operations.find(
              operation => operation.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(3);
        expect(
          asPlainObject(
            result.compile.compiledGraph.operations
              .find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              )
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "C"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_C_assertions_rowConditions")
        ]);
      });

      test("Assertions added through includeDependentAssertions and explicitly listed in dependencies are deduplicated.", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependencies: ["A_assert"]
}
select * from \${ref({name: "A", includeDependentAssertions: true})}
select 1 as btest`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables.find(
              table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
            ).dependencyTargets.length
          )
        ).equals(3);
        expect(
          asPlainObject(
            result.compile.compiledGraph.tables
              .find(table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B"))
              .dependencyTargets.flatMap(dependencyTarget => dependencyTarget.name)
          )
        ).deep.equals([
          prefixAdjustedName(testConfig.namePrefix, "A_assert"),
          prefixAdjustedName(testConfig.namePrefix, "A"),
          prefixAdjustedName(testConfig.namePrefix, "defaultDataset_A_assertions_rowConditions")
        ]);
      });

      test("When includeDependentAssertions property in config and ref are set differently for the same dependency, compilation error is thrown.", () => {
        fs.writeFileSync(
          path.join(projectDir, "definitions/B.sqlx"),
          `
config {
  type: "table",
  dependencies: [{name: "A", includeDependentAssertions: false}, {name: "C", includeDependentAssertions: true}]
}
select * from \${ref({name: "A", includeDependentAssertions: true})}
select * from \${ref({name: "C", includeDependentAssertions: false})}
select 1 as btest`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/C.sqlx"),
          `
config {
  type: "table",
    assertions: {
      rowConditions: ["test > 1"]
  }
}
SELECT 1 as test
}`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).deep.equals(2);
        expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).deep.equals(
          `Conflicting "includeDependentAssertions" properties are not allowed. Dependency A has different values set for this property.`
        );
      });

      suite("Action configs", () => {
        test(`When dependOnDependencyAssertions property is set to true, assertions from A are added as dependencies`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
- operation:
    filename: C.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
- notebook:
    filename: notebook.ipynb
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
          expect(
            asPlainObject(
              result.compile.compiledGraph.operations.find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.tables.find(
                table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.notebooks.find(
                notebook =>
                  notebook.target.name === prefixAdjustedName(testConfig.namePrefix, "notebook")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        });

        test(`Setting includeDependentAssertions to true in config.dependencies adds assertions from that dependency to dependencyTargets`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependencyTargets:
      - name: A
        includeDependentAssertions: true 
- operation:
    filename: C.sql
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
- notebook:
    filename: notebook.ipynb
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(
            asPlainObject(
              result.compile.compiledGraph.operations.find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.tables.find(
                table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.notebooks.find(
                notebook =>
                  notebook.target.name === prefixAdjustedName(testConfig.namePrefix, "notebook")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
        });

        test(`When dependOnDependencyAssertions=true and includeDependentAssertions=false, the assertions related to dependency should not be added to dependencyTargets`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
        includeDependentAssertions: false
- assertion:
    filename: B_assert.sql
    dependencyTargets:
      - name: B
- operation:
    filename: C.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
        includeDependentAssertions: false
- notebook:
    filename: notebook.ipynb
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
        includeDependentAssertions: false
      - name: B
`
          );
          fs.writeFileSync(path.join(projectDir, "definitions/B_assert.sql"), "SELECT test from B");

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(
            asPlainObject(
              result.compile.compiledGraph.operations.find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
              ).dependencyTargets.length
            )
          ).deep.equals(1);
          expect(
            asPlainObject(
              result.compile.compiledGraph.tables.find(
                table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              ).dependencyTargets.length
            )
          ).deep.equals(1);
          expect(
            asPlainObject(
              result.compile.compiledGraph.notebooks.find(
                notebook =>
                  notebook.target.name === prefixAdjustedName(testConfig.namePrefix, "notebook")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
        });

        test(`When dependOnDependencyAssertions=false and includeDependentAssertions=true, the assertions related to dependency should be added to dependencyTargets`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependOnDependencyAssertions: false
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
- assertion:
    filename: B_assert.sql
    dependencyTargets:
      - name: B
- operation:
    filename: C.sql
    dependOnDependencyAssertions: false
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
      - name: B
- notebook:
    filename: notebook.ipynb
    dependOnDependencyAssertions: false
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
      - name: B
`
          );
          fs.writeFileSync(path.join(projectDir, "definitions/B_assert.sql"), "SELECT test from B");

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(
            asPlainObject(
              result.compile.compiledGraph.operations.find(
                operation =>
                  operation.target.name === prefixAdjustedName(testConfig.namePrefix, "C")
              ).dependencyTargets.length
            )
          ).deep.equals(4);
          expect(
            asPlainObject(
              result.compile.compiledGraph.tables.find(
                table => table.target.name === prefixAdjustedName(testConfig.namePrefix, "B")
              ).dependencyTargets.length
            )
          ).deep.equals(3);
          expect(
            asPlainObject(
              result.compile.compiledGraph.notebooks.find(
                notebook =>
                  notebook.target.name === prefixAdjustedName(testConfig.namePrefix, "notebook")
              ).dependencyTargets.length
            )
          ).deep.equals(4);
        });

        test(`When includeDependentAssertions property in config and ref are set differently for the same dependency, compilation error is thrown.`, () => {
          fs.writeFileSync(
            path.join(projectDir, "definitions/actions.yaml"),
            `
actions:
- view:
    filename: B.sql
    dependOnDependencyAssertions: true
    dependencyTargets:
      - name: A
- operation:
    filename: C.sql
    dependencyTargets:
      - name: A
        includeDependentAssertions: true
      - name: B
      - name: A
        includeDependentAssertions: false
`
          );
          fs.writeFileSync(path.join(projectDir, "definitions/B_assert.sql"), "SELECT test from B");

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).deep.equals(1);
          expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).deep.equals(
            `Conflicting "includeDependentAssertions" properties are not allowed. Dependency A has different values set for this property.`
          );
        });
      });
    });
  });
});

function coreExecutionRequestFromPath(projectDir: string): dataform.CoreExecutionRequest {
  return dataform.CoreExecutionRequest.create({
    compile: { compileConfig: { projectDir, filePaths: walkDirectoryForFilenames(projectDir) } }
  });
}

// A VM is needed when running main because Node functions like `require` are overridden.
function runMainInVm(
  coreExecutionRequest: dataform.CoreExecutionRequest
): dataform.CoreExecutionResponse {
  const projectDir = coreExecutionRequest.compile.compileConfig.projectDir;

  // Copy over the build Dataform Core that is set up as a node_modules directory.
  fs.copySync(`${process.cwd()}/core/node_modules`, `${projectDir}/node_modules`);

  const compiler = compile as CompilerFunction;
  // Then use vm2's native compiler integration to apply the compiler to files.
  const nodeVm = new NodeVM({
    // Inheriting the console makes console.logs show when tests are running, which is useful for
    // debugging.
    console: "inherit",
    wrapper: "none",
    require: {
      builtin: ["path"],
      context: "sandbox",
      external: true,
      root: projectDir,
      resolve: (moduleName, parentDirName) =>
        path.join(parentDirName, path.relative(parentDirName, projectDir), moduleName)
    },
    sourceExtensions: SOURCE_EXTENSIONS,
    compiler
  });

  const encodedCoreExecutionRequest = encode64(dataform.CoreExecutionRequest, coreExecutionRequest);
  const vmIndexFileName = path.resolve(path.join(projectDir, "index.js"));
  const encodedCoreExecutionResponse = nodeVm.run(
    `return require("@dataform/core").main("${encodedCoreExecutionRequest}")`,
    vmIndexFileName
  );
  return decode64(dataform.CoreExecutionResponse, encodedCoreExecutionResponse);
}

function walkDirectoryForFilenames(projectDir: string, relativePath: string = ""): string[] {
  let paths: string[] = [];
  fs.readdirSync(path.join(projectDir, relativePath), { withFileTypes: true })
    .filter(directoryEntry => directoryEntry.name !== "node_modules")
    .forEach(directoryEntry => {
      if (directoryEntry.isDirectory()) {
        paths = paths.concat(walkDirectoryForFilenames(projectDir, directoryEntry.name));
        return;
      }
      const fileExtension = directoryEntry.name.split(".").slice(-1)[0];
      if (directoryEntry.isFile() && SOURCE_EXTENSIONS.includes(fileExtension)) {
        paths.push(directoryEntry.name);
      }
    });
  return paths.map(filename => path.join(relativePath, filename));
}

function prefixAdjustedName(prefix: string | undefined, name: string) {
  return prefix ? `${prefix}_${name}` : name;
}
