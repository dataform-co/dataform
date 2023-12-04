import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";
import { CompilerFunction, NodeVM } from "vm2";

import { decode64, encode64 } from "df/common/protos";
import { compile } from "df/core/compilers";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import { asPlainObject } from "df/tests/utils";

const VALID_WORKFLOW_SETTINGS_YAML = `
defaultDatabase: dataform
`;

const VALID_DATAFORM_JSON = `
{
  "warehouse": "bigquery",
  "defaultDatabase": "dataform"
}
`;

const EMPTY_NOTEBOOK_CONTENTS = '{ "cells": [] }';

suite("@dataform/core", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("workflow settings", () => {
    test(`main succeeds when a valid workflow_settings.yaml is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      const result = runMainInVm(coreExecutionRequest);

      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
        asPlainObject({
          warehouse: "bigquery",
          defaultDatabase: "dataform"
        })
      );
    });

    // dataform.json for workflow settings is deprecated, but still currently supported.
    test(`main succeeds when a valid dataform.json is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      const result = runMainInVm(coreExecutionRequest);

      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
        asPlainObject({
          warehouse: "bigquery",
          defaultDatabase: "dataform"
        })
      );
    });

    test(`main fails when no workflow settings file is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "Failed to resolve workflow_settings.yaml"
      );
    });

    test(`main fails when both workflow settings and dataform.json files are present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
      );
    });

    test(`main fails when workflow_settings.yaml is an invalid yaml file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "&*19132sdS:asd:");
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw("workflow_settings.yaml is invalid");
    });

    test(`main fails when dataform.json is an invalid json file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(path.join(projectDir, "dataform.json"), '{keyWithNoQuotes: "validValue"}');
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "Unexpected token k in JSON at position 1"
      );
    });

    test(`main fails when a valid workflow_settings.yaml contains unknown fields`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        "notAProjectConfigField: value"
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "Workflow settings error: unexpected key 'notAProjectConfigField'"
      );
    });

    test(`main fails when a valid dataform.json contains unknown fields`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "dataform.json"),
        `{"notAProjectConfigField": "value"}`
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "Workflow settings error: unexpected key 'notAProjectConfigField'"
      );
    });

    suite("variables", () => {
      test(`variables in workflow_settings.yaml must be strings`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          `
vars:
  intValue: 1
  strValue: "str"`
        );
        const coreExecutionRequest = dataform.CoreExecutionRequest.create({
          compile: { compileConfig: { projectDir } }
        });

        expect(() => runMainInVm(coreExecutionRequest)).to.throw(
          "Custom variables defined in workflow settings can only be strings."
        );
      });

      test(`variables in dataform.json must be strings`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        fs.writeFileSync(
          path.join(projectDir, "dataform.json"),
          `{"vars": { "intVar": 1, "strVar": "str" } }`
        );
        const coreExecutionRequest = dataform.CoreExecutionRequest.create({
          compile: { compileConfig: { projectDir } }
        });

        expect(() => runMainInVm(coreExecutionRequest)).to.throw(
          "Custom variables defined in workflow settings can only be strings."
        );
      });

      test(`variables can be referenced in SQLX`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          `
defaultLocation: "us"
vars:
  var1: value1
  var2: value2
  var3: value3`
        );
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        fs.mkdirSync(path.join(projectDir, "definitions"));
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        fs.writeFileSync(
          path.join(projectDir, "definitions/file.sqlx"),
          // TODO(https://github.com/dataform-co/dataform/issues/1295): add a test and fix
          // functionality for assertions overriding database.
          `
config {
  type: "table",
  database: dataform.projectConfig.vars.var1,
  schema: "tableSchema",
  description: dataform.projectConfig.vars.var2,
  assertions: {
    nonNull: [dataform.projectConfig.vars.var3],
  }
}
select 1 AS \${dataform.projectConfig.vars.var3}`
        );
        const coreExecutionRequest = dataform.CoreExecutionRequest.create({
          compile: { compileConfig: { projectDir, filePaths: ["definitions/file.sqlx"] } }
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
                    database: "value1",
                    name: "file",
                    schema: "tableSchema"
                  }
                ],
                fileName: "definitions/file.sqlx",
                parentAction: {
                  database: "value1",
                  name: "file",
                  schema: "tableSchema"
                },
                query:
                  "\nSELECT\n  'value3 IS NOT NULL' AS failing_row_condition,\n  *\nFROM `value1.tableSchema.file`\nWHERE NOT (value3 IS NOT NULL)\n",
                target: {
                  name: "tableSchema_file_assertions_rowConditions"
                }
              }
            ],
            dataformCoreVersion: "3.0.0",
            graphErrors: {},
            projectConfig: {
              defaultLocation: "us",
              vars: {
                var1: "value1",
                var2: "value2",
                var3: "value3"
              },
              warehouse: "bigquery"
            },
            tables: [
              {
                actionDescriptor: {
                  description: "value2"
                },
                canonicalTarget: {
                  database: "value1",
                  name: "file",
                  schema: "tableSchema"
                },
                disabled: false,
                enumType: "TABLE",
                fileName: "definitions/file.sqlx",
                query: "\n\nselect 1 AS value3",
                target: {
                  database: "value1",
                  name: "file",
                  schema: "tableSchema"
                },
                type: "table"
              }
            ],
            targets: [
              {
                database: "value1",
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

      test(`variables from the project config override are merged with dataform.json variables and applied`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          `
defaultDatabase: dataform
defaultLocation: locationInWorkflowSettings
vars:
  selectVar: selectVal
`
        );
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        fs.mkdirSync(path.join(projectDir, "definitions"));
        // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
        fs.writeFileSync(
          path.join(projectDir, "definitions/file.sqlx"),
          `
config {
  type: "table",
  database: dataform.projectConfig.vars.databaseVar,
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
                  databaseVar: "databaseVal"
                }
              }
            }
          }
        });

        const result = runMainInVm(coreExecutionRequest);

        expect(asPlainObject(result.compile.compiledGraph)).deep.equals(
          asPlainObject({
            dataformCoreVersion: "3.0.0",
            graphErrors: {},
            projectConfig: {
              defaultDatabase: "dataform",
              defaultLocation: "locationInOverride",
              vars: {
                databaseVar: "databaseVal",
                selectVar: "selectVal"
              },
              warehouse: "bigquery"
            },
            tables: [
              {
                canonicalTarget: {
                  database: "databaseVal",
                  name: "file"
                },
                disabled: false,
                enumType: "TABLE",
                fileName: "definitions/file.sqlx",
                query: "\n\nselect 1 AS selectVal",
                target: {
                  database: "databaseVal",
                  name: "file"
                },
                type: "table"
              }
            ],
            targets: [
              {
                database: "databaseVal",
                name: "file"
              }
            ]
          })
        );
      });
    });
  });

  suite("notebooks", () => {
    test(`notebooks can be loaded via an actions config file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.mkdirSync(path.join(projectDir, "definitions"));
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
  - fileName: definitions/notebook.ipynb`
      );
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "definitions/notebook.ipynb"),
        EMPTY_NOTEBOOK_CONTENTS
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: {
          compileConfig: {
            projectDir,
            filePaths: ["definitions/actions.yaml"]
          }
        }
      });

      const result = runMainInVm(coreExecutionRequest);

      expect(asPlainObject(result.compile.compiledGraph.notebooks)).deep.equals(
        asPlainObject([
          {
            config: {
              fileName: "definitions/notebook.ipynb",
              target: {
                database: "dataform",
                name: "note"
              }
            },
            notebookContents: '{"cells":[]})',
            target: {
              database: "dataform",
              name: "note"
            }
          }
        ])
      );
    });
  });
});

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
    sourceExtensions: ["js", "sql", "sqlx", "yaml", "ipynb"],
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
