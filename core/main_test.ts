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
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import { asPlainObject } from "df/tests/utils";

const SOURCE_EXTENSIONS = ["js", "sql", "sqlx", "yaml", "ipynb"];

const VALID_WORKFLOW_SETTINGS_YAML = `
defaultProject: dataform
defaultLocation: US
`;

const VALID_DATAFORM_JSON = `
{
  "defaultDatabase": "dataform"
}
`;

class TestConfigs {
  public static bigquery = dataform.WorkflowSettings.create({
    defaultDataset: "schema",
    defaultLocation: "US"
  });

  public static bigqueryWithDefaultDatabase = dataform.WorkflowSettings.create({
    ...TestConfigs.bigquery,
    defaultProject: "default-database"
  });

  public static bigqueryWithDatasetSuffix = dataform.WorkflowSettings.create({
    ...TestConfigs.bigquery,
    datasetSuffix: "suffix"
  });

  public static bigqueryWithDefaultProjectAndDataset = dataform.WorkflowSettings.create({
    ...TestConfigs.bigqueryWithDefaultDatabase,
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
          expect(result.compile.compiledGraph.operations[0].queries[0]).deep.equals(
            `\`schema${suffix}.${prefix}e\``
          );
        });
      });
    });

    suite("resolve fails", () => {
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
        { ...TestConfigs.bigqueryWithDatasetSuffix, defaultProject: "default-database" },
        { ...TestConfigs.bigqueryWithNamePrefix, defaultProject: "default-database" }
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
              `default-database${testConfig.projectSuffix ? `_suffix` : ""}.` +
                `schema${testConfig.datasetSuffix ? `_suffix` : ""}.` +
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

      expect(asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors)).deep.equals(
        []
      );
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

      expect(asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors)).deep.equals(
        []
      );
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

      expect(asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors)).deep.equals(
        []
      );
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

      expect(asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors)).deep.equals(
        []
      );
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

      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
        asPlainObject({
          warehouse: "bigquery",
          defaultDatabase: "dataform",
          defaultLocation: "US"
        })
      );
      expect(asPlainObject(result.compile.compiledGraph.graphErrors.compilationErrors)).deep.equals(
        []
      );
    });

    // dataform.json for workflow settings is deprecated, but still currently supported.
    test(`main succeeds when a valid dataform.json is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
        asPlainObject({
          defaultDatabase: "dataform"
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
        "Cannot find field: notAProjectConfigField in message"
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
        "Cannot find field: notAProjectConfigField in message"
      );
    });

    test(`workflow settings and project config overrides are merged and applied within SQLX files`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `
defaultProject: dataform
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

      expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
        asPlainObject({
          dataformCoreVersion: version,
          graphErrors: {},
          projectConfig: {
            defaultDatabase: "dataform",
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
defaultProject: dataform`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
          asPlainObject({
            warehouse: "bigquery",
            defaultDatabase: "dataform"
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

      expect(asPlainObject(result.compile.compiledGraph.notebooks)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "dataform",
              name: "notebook"
            },
            canonicalTarget: {
              database: "dataform",
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

      expect(asPlainObject(result.compile.compiledGraph.notebooks)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "dataform",
              name: "notebook"
            },
            fileName: "definitions/notebook.ipynb",
            notebookContents: JSON.stringify({
              cells: [
                { cell_type: "markdown", source: ["# Some title"], outputs: [] },
                { cell_type: "code", source: ["print('hi')"], outputs: [] },
                { cell_type: "raw", source: ["print('hi')"] }
              ]
            }),
            target: {
              database: "dataform",
              name: "notebook"
            }
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

      expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "dataform",
              name: "action"
            },
            fileName: "definitions/action.sql",
            queries: ["SELECT 1"],
            target: {
              database: "dataform",
              name: "action"
            }
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

      expect(asPlainObject(result.compile.compiledGraph.declarations)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "dataform",
              name: "action"
            },
            target: {
              database: "dataform",
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
        "Cannot find field: fileName in message, or value type is incorrect"
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

      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "dataform",
              name: "action"
            },
            fileName: "definitions/action.sql",
            query: "SELECT 1",
            target: {
              database: "dataform",
              name: "action"
            },
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

      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "dataform",
              name: "action"
            },
            fileName: "definitions/action.sql",
            query: "SELECT 1",
            incrementalQuery: "SELECT 1",
            target: {
              database: "dataform",
              name: "action"
            },
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

      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "dataform",
              name: "action"
            },
            fileName: "definitions/action.sql",
            query: "SELECT 1",
            target: {
              database: "dataform",
              name: "action"
            },
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

      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "dataform",
              name: "action"
            },
            fileName: "definitions/action.sql",
            query: "SELECT 1",
            target: {
              database: "dataform",
              name: "action"
            }
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
        "Cannot find field: materialized in message, or value type is incorrect"
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

      expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
        asPlainObject([
          {
            canonicalTarget: {
              database: "dataform",
              name: "utf8characters:私🙂 and some spaces"
            },
            fileName: "definitions/utf8characters:私🙂 and some spaces.sql",
            queries: ["SELECT 1"],
            target: {
              database: "dataform",
              name: "utf8characters:私🙂 and some spaces"
            }
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
