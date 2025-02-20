// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml } from "js-yaml";
import * as path from "path";

import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_DATAFORM_JSON,
  VALID_WORKFLOW_SETTINGS_YAML,
  WorkflowSettingsTemplates
} from "df/testing/run_core";

const EMPTY_NOTEBOOK_CONTENTS = '{ "cells": [] }';

// INFO: if you want to see an overview of the tests in this file, press cmd-k-3 while in
// VSCode, to collapse everything below the third level of indentation.
suite("@dataform/core", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("session", () => {
    suite("resolve succeeds", () => {
      [
        WorkflowSettingsTemplates.bigquery,
        WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
        WorkflowSettingsTemplates.bigqueryWithNamePrefix
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

    test("fails when cannot resolve", () => {
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

    test("fails when ambiguous resolve", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("a", {"schema": "foo"})
publish("a", {"schema": "bar"})
publish("b", {"schema": "foo"}).dependencies("a")`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
      ).deep.equals([
        `Ambiguous Action name: {\"name\":\"a\",\"includeDependentAssertions\":false}. Did you mean one of: foo.a, bar.a.`
      ]);
    });

    suite("context methods", () => {
      [
        WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
        {
          ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
          defaultProject: "defaultProject"
        },
        { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
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
          dumpYaml(dataform.WorkflowSettings.create(WorkflowSettingsTemplates.bigquery))
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
        `Action target datasets cannot include '.'`,
        `Action target names cannot include '.'`,
        `Action target names cannot include '.'`,
        `Action target names cannot include '.'`
      ]);
    });

    test("fails when non-unique target", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("name")
publish("name")`
      );

      const result = runMainInVm(
        coreExecutionRequestFromPath(
          projectDir,
          dataform.ProjectConfig.create({
            defaultSchema: "otherDataset"
          })
        )
      );

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
      ).deep.equals([
        `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
        `Duplicate canonical target detected. Canonical targets must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
        `Duplicate action name detected. Names within a schema must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`,
        `Duplicate canonical target detected. Canonical targets must be unique across tables, declarations, assertions, and operations:\n\"{\"schema\":\"otherDataset\",\"name\":\"name\",\"database\":\"defaultProject\"}\"`
      ]);
    });

    test("fails when circular dependencies", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("a").dependencies("b")
publish("b").dependencies("a")`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
      ).deep.equals([
        `Circular dependency detected in chain: [{\"database\":\"defaultProject\",\"name\":\"a\",\"schema\":\"defaultDataset\"} > {\"database\":\"defaultProject\",\"name\":\"b\",\"schema\":\"defaultDataset\"} > defaultProject.defaultDataset.a]`
      ]);
    });

    test("fails when missing dependency", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, "definitions/file.sql"), "unused");
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("a").dependencies("b")`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
      ).deep.equals([
        `Missing dependency detected: Action \"defaultProject.defaultDataset.a\" depends on \"{\"name\":\"b\",\"includeDependentAssertions\":false}\" which does not exist`
      ]);
    });

    test("semi-colons at the end of SQL statements throws", () => {
      // If this didn't happen, then the generated SQL could be incorrect
      // because of being broken up by semi-colons.

      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/file.js"),
        `
publish("a", "SELECT 1;\\n");
publish("b", "SELECT 1;");`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
      ).deep.equals([
        "Semi-colons are not allowed at the end of SQL statements.",
        "Semi-colons are not allowed at the end of SQL statements."
      ]);
    });
  });

  suite("sqlx special characters", () => {
    test("extract blocks", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        dumpYaml(dataform.WorkflowSettings.create(WorkflowSettingsTemplates.bigquery))
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
        dumpYaml(dataform.WorkflowSettings.create(WorkflowSettingsTemplates.bigquery))
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
        dumpYaml(dataform.WorkflowSettings.create(WorkflowSettingsTemplates.bigquery))
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
        dumpYaml(dataform.WorkflowSettings.create(WorkflowSettingsTemplates.bigquery))
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
    test(`valid workflow_settings.yaml is present`, () => {
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
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
      );
    });

    test(`fails when workflow_settings.yaml cannot be represented in JSON format`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "&*19132sdS:asd:");

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "workflow_settings.yaml is invalid"
      );
    });

    test(`fails when workflow settings fails to be parsed`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
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
        "Unexpected token k in JSON at position 1"
      );
    });

    test(`fails when a valid workflow_settings.yaml contains unknown fields`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        "notAProjectConfigField: value"
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        `Workflow settings error: Unexpected property "notAProjectConfigField", or property value type of "string" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-WorkflowSettings for allowed properties.`
      );
    });

    test(`fails when a valid workflow_settings.yaml base level is an array`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "- someArrayEntry");

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        "Expected a top-level object, but found an array"
      );
    });

    test(`fails when a valid dataform.json contains unknown fields`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "dataform.json"),
        `{"notAProjectConfigField": "value"}`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        `Dataform json error: Unexpected property "notAProjectConfigField", or property value type of "string" is incorrect.`
      );
    });

    test("fails when defaultLocation is not present in workflow_settings.yaml", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `
defaultProject: defaultProject
defaultDataset: defaultDataset`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors?.map(error => error.message)
      ).deep.equals([
        `A defaultLocation is required for BigQuery. This can be configured in workflow_settings.yaml.`
      ]);
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

  suite("action configs", () => {
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

    test(`fails when empty objects are given`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:`
      );

      expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
        `Unexpected empty value for "actions". See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-ActionConfigs for allowed properties.`
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
            queries: ["SELECT 1"],
            hermeticity: "NON_HERMETIC"
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
      dataset: defaultDataset
      project: defaultProject
- incrementalTable:
    filename: incrementalTable.sql
    dependencyTargets:
    - name: table
      dataset: defaultDataset
      project: defaultProject
- view:
    filename: view.sql
    dependencyTargets:
    - name: incrementalTable
      dataset: defaultDataset
      project: defaultProject
- operation:
    filename: operation.sql
    dependencyTargets:
    - name: view
      dataset: defaultDataset
      project: defaultProject
- notebook:
    filename: notebook.ipynb
    dependencyTargets:
    - name: view
      dataset: defaultDataset
      project: defaultProject`
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

    test(`dependency targets of actions with different types are loaded`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      // The dependency target for depending on a notebook currently hacks around the limitations of
      // the target proto, until proper target support for notebooks is added.
      fs.writeFileSync(
        path.join(projectDir, "definitions/actions.yaml"),
        `
actions:
- notebook:
    name: notebook1
    location: location
    project: project
    filename: notebook.ipynb
- operation:
    name: operation1
    dataset: dataset
    project: project
    dependencyTargets:
    - name: notebook1
      dataset: location
      project: project
    filename: operation.sql`
      );
      fs.writeFileSync(path.join(projectDir, "definitions/operation.sql"), "SELECT 1");
      fs.writeFileSync(
        path.join(projectDir, `definitions/notebook.ipynb`),
        EMPTY_NOTEBOOK_CONTENTS
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    });
  });

  suite("javascript API", () => {
    suite("publish", () => {
      ["table", "view", "incremental"].forEach(tableType => {
        [
          WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
          {
            ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
            defaultProject: "defaultProject"
          },
          { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
        ].forEach(projectConfig => {
          test(
            `publish for table type ${tableType}, with project suffix ` +
              `'${projectConfig.projectSuffix}', dataset suffix ` +
              `'${projectConfig.datasetSuffix}', and name prefix '${projectConfig.namePrefix}'`,
            () => {
              const projectDir = tmpDirFixture.createNewTmpDir();
              fs.writeFileSync(
                path.join(projectDir, "workflow_settings.yaml"),
                dumpYaml(dataform.WorkflowSettings.create(projectConfig))
              );
              fs.mkdirSync(path.join(projectDir, "definitions"));
              fs.writeFileSync(
                path.join(projectDir, "definitions/publish.js"),
                `
publish("name", {
  type: "${tableType}",
}).query(_ => "SELECT 1")
  .preOps(_ => ["pre_op"])
  .postOps(_ => ["post_op"])
  .database("otherProject")
  .schema("otherDataset")`
              );

              const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

              expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
              expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
                asPlainObject([
                  {
                    type: tableType,
                    hermeticity: "NON_HERMETIC",
                    target: {
                      database: projectConfig.projectSuffix
                        ? `otherProject_${projectConfig.projectSuffix}`
                        : "otherProject",
                      schema: projectConfig.datasetSuffix
                        ? `otherDataset_${projectConfig.datasetSuffix}`
                        : "otherDataset",
                      name: projectConfig.namePrefix ? `${projectConfig.namePrefix}_name` : "name"
                    },
                    canonicalTarget: {
                      database: projectConfig.defaultProject,
                      schema: projectConfig.defaultDataset,
                      name: "name"
                    },
                    disabled: false,
                    enumType: tableType.toUpperCase(),
                    fileName: "definitions/publish.js",
                    query: "SELECT 1",
                    postOps: ["post_op"],
                    preOps: ["pre_op"],
                    ...(tableType === "incremental"
                      ? {
                          incrementalPostOps: ["post_op"],
                          incrementalPreOps: ["pre_op"],
                          incrementalQuery: "SELECT 1",
                          protected: true,
                          onSchemaChange: "IGNORE"
                        }
                      : {})
                  }
                ])
              );
            }
          );
        });

        test("ref resolved correctly", () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          fs.writeFileSync(
            path.join(projectDir, "workflow_settings.yaml"),
            VALID_WORKFLOW_SETTINGS_YAML
          );
          fs.mkdirSync(path.join(projectDir, "definitions"));
          fs.writeFileSync(
            path.join(projectDir, "definitions/operation.sqlx"),
            `
config {
  hasOutput: true
}
SELECT 1`
          );
          fs.writeFileSync(
            path.join(projectDir, "definitions/publish.js"),
            `
publish("name", {
  type: "${tableType}",
}).query(ctx => \`SELECT * FROM \${ctx.ref('operation')}\`)`
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
          expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
            {
              canonicalTarget: {
                database: "defaultProject",
                name: "name",
                schema: "defaultDataset"
              },
              dependencyTargets: [
                {
                  database: "defaultProject",
                  name: "operation",
                  schema: "defaultDataset"
                }
              ],
              disabled: false,
              enumType: tableType.toUpperCase(),
              fileName: "definitions/publish.js",
              hermeticity: "NON_HERMETIC",
              query: "SELECT * FROM `defaultProject.defaultDataset.operation`",
              target: {
                database: "defaultProject",
                name: "name",
                schema: "defaultDataset"
              },
              type: tableType,
              ...(tableType === "incremental"
                ? {
                    incrementalQuery: "SELECT * FROM `defaultProject.defaultDataset.operation`",
                    protected: true,
                    onSchemaChange: "IGNORE"
                  }
                : {})
            }
          ]);
        });
      });
    });

    suite("operate", () => {
      [
        WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
        {
          ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
          defaultProject: "defaultProject"
        },
        { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
      ].forEach(projectConfig => {
        test(
          `operate with project suffix ` +
            `'${projectConfig.projectSuffix}', dataset suffix ` +
            `'${projectConfig.datasetSuffix}', and name prefix '${projectConfig.namePrefix}'`,
          () => {
            const projectDir = tmpDirFixture.createNewTmpDir();
            fs.writeFileSync(
              path.join(projectDir, "workflow_settings.yaml"),
              dumpYaml(dataform.WorkflowSettings.create(projectConfig))
            );
            fs.mkdirSync(path.join(projectDir, "definitions"));
            fs.writeFileSync(
              path.join(projectDir, "definitions/operate.js"),
              `
operate("name", {
  type: "operations",
}).queries(_ => ["SELECT 1", "SELECT 2"])
  .database("otherProject")
  .schema("otherDataset")`
            );

            const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

            expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
            expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
              asPlainObject([
                {
                  target: {
                    database: projectConfig.projectSuffix
                      ? `otherProject_${projectConfig.projectSuffix}`
                      : "otherProject",
                    schema: projectConfig.datasetSuffix
                      ? `otherDataset_${projectConfig.datasetSuffix}`
                      : "otherDataset",
                    name: projectConfig.namePrefix ? `${projectConfig.namePrefix}_name` : "name"
                  },
                  canonicalTarget: {
                    database: projectConfig.defaultProject,
                    schema: projectConfig.defaultDataset,
                    name: "name"
                  },
                  fileName: "definitions/operate.js",
                  hermeticity: "NON_HERMETIC",
                  queries: ["SELECT 1", "SELECT 2"]
                }
              ])
            );
          }
        );
      });

      test("ref resolved correctly", () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, "definitions/table.sqlx"),
          `config {type: "table"} SELECT 1`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/operate.js"),
          `
operate("name", {
  type: "operations",
}).queries(ctx => [\`SELECT * FROM \${ctx.ref('table')}\`])`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals([
          {
            canonicalTarget: {
              database: "defaultProject",
              name: "name",
              schema: "defaultDataset"
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                name: "table",
                schema: "defaultDataset"
              }
            ],
            fileName: "definitions/operate.js",
            hermeticity: "NON_HERMETIC",
            queries: ["SELECT * FROM `defaultProject.defaultDataset.table`"],
            target: {
              database: "defaultProject",
              name: "name",
              schema: "defaultDataset"
            }
          }
        ]);
      });
    });

    suite("assert", () => {
      [
        WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset,
        {
          ...WorkflowSettingsTemplates.bigqueryWithDatasetSuffix,
          defaultProject: "defaultProject"
        },
        { ...WorkflowSettingsTemplates.bigqueryWithNamePrefix, defaultProject: "defaultProject" }
      ].forEach(projectConfig => {
        test(
          `assert with project suffix ` +
            `'${projectConfig.projectSuffix}', dataset suffix ` +
            `'${projectConfig.datasetSuffix}', and name prefix '${projectConfig.namePrefix}'`,
          () => {
            const projectDir = tmpDirFixture.createNewTmpDir();
            fs.writeFileSync(
              path.join(projectDir, "workflow_settings.yaml"),
              dumpYaml(dataform.WorkflowSettings.create(projectConfig))
            );
            fs.mkdirSync(path.join(projectDir, "definitions"));
            fs.writeFileSync(
              path.join(projectDir, "definitions/assert.js"),
              `
assert("name", {
  type: "operations",
}).query(_ => "SELECT 1")
  .database("otherProject")
  .schema("otherDataset")`
            );

            const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

            expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
            expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
              asPlainObject([
                {
                  target: {
                    database: projectConfig.projectSuffix
                      ? `otherProject_${projectConfig.projectSuffix}`
                      : "otherProject",
                    schema: projectConfig.datasetSuffix
                      ? `otherDataset_${projectConfig.datasetSuffix}`
                      : "otherDataset",
                    name: projectConfig.namePrefix ? `${projectConfig.namePrefix}_name` : "name"
                  },
                  canonicalTarget: {
                    database: projectConfig.defaultProject,
                    schema: projectConfig.defaultDataset,
                    name: "name"
                  },
                  fileName: "definitions/assert.js",
                  query: "SELECT 1"
                }
              ])
            );
          }
        );
      });

      test("ref resolved correctly", () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(
          path.join(projectDir, "definitions/table.sqlx"),
          `config {type: "table"} SELECT 1`
        );
        fs.writeFileSync(
          path.join(projectDir, "definitions/assert.js"),
          `
assert("name", {
  type: "assert",
}).query(ctx => \`SELECT * FROM \${ctx.ref('table')}\`)`
        );

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals([
          {
            canonicalTarget: {
              database: "defaultProject",
              name: "name",
              schema: "defaultDataset"
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                name: "table",
                schema: "defaultDataset"
              }
            ],
            fileName: "definitions/assert.js",
            query: "SELECT * FROM `defaultProject.defaultDataset.table`",
            target: {
              database: "defaultProject",
              name: "name",
              schema: "defaultDataset"
            }
          }
        ]);
      });
    });

    suite("invalid options", () => {
      [
        {
          testName: "partitionBy invalid for BigQuery views",
          fileContents: `
publish("name", {
  type: "view",
  bigquery: {
    partitionBy: "some_partition"
  }
})`,
          expectedError:
            'Unexpected property "partitionBy" in BigQuery view config. Supported properties are: ["labels","additionalOptions"]'
        },
        {
          testName: "clusterBy invalid for BigQuery views",
          fileContents: `
publish("name", {
  type: "view",
  bigquery: {
    clusterBy: ["some_cluster"]
  }
})`,
          expectedError:
            'Unexpected property "clusterBy" in BigQuery view config. Supported properties are: ["labels","additionalOptions"]'
        },
        {
          testName: "partitionExpirationDays invalid for BigQuery views",
          fileContents: `
publish("name", {
  type: "view",
  bigquery: {
    partitionExpirationDays: 7
  }
})`,
          expectedError:
            'Unexpected property "partitionExpirationDays" in BigQuery view config. Supported properties are: ["labels","additionalOptions"]'
        },
        {
          testName: "requirePartitionFilter invalid for BigQuery views",
          fileContents: `
publish("name", {
  type: "view",
  bigquery: {
    requirePartitionFilter: true
  }
})`,
          expectedError:
            'Unexpected property "requirePartitionFilter" in BigQuery view config. Supported properties are: ["labels","additionalOptions"]'
        },
        {
          testName: "partitionExpirationDays invalid for BigQuery materialized views",
          fileContents: `
publish("name", {
  type: "view",
  materialized: true,
  bigquery: {
    partitionExpirationDays: 7
  }
})`,
          expectedError:
            'Unexpected property "partitionExpirationDays" in BigQuery view config. Supported properties are: ["labels","additionalOptions"]'
        },
        {
          testName: "requirePartitionFilter invalid for BigQuery materialized views",
          fileContents: `
publish("name", {
  type: "view",
  materialized: true,
  bigquery: {
    requirePartitionFilter: true
  }
})`,
          expectedError:
            'Unexpected property "requirePartitionFilter" in BigQuery view config. Supported properties are: ["labels","additionalOptions"]'
        },
        {
          testName: "materialized invalid for BigQuery tables",
          fileContents: `
publish("name", {
  type: "table",
  materialized: true,
})`,
          expectedError:
            'Unexpected property "materialized", or property value type of "boolean" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-ActionConfig-TableConfig for allowed properties.'
        },
        {
          testName: "partitionExpirationDays invalid for BigQuery tables",
          fileContents: `
publish("name", {
  type: "table",
  bigquery: {
    partitionExpirationDays: 7
  }
})`,
          expectedError:
            "requirePartitionFilter/partitionExpirationDays are not valid for non partitioned BigQuery tables"
        },
        {
          testName: "duplicate partitionExpirationDays is invalid",
          fileContents: `
publish("name", {
  type: "table",
  bigquery: {
    partitionBy: "partition",
    partitionExpirationDays: 1,
    additionalOptions: {
      partition_expiration_days: "7"
    }
  }
})`,
          expectedError: "partitionExpirationDays has been declared twice"
        },
        {
          testName: "duplicate requirePartitionFilter is invalid",
          fileContents: `
publish("name", {
  type: "table",
  bigquery: {
    partitionBy: "partition",
    requirePartitionFilter: true,
    additionalOptions: {
      require_partition_filter: "false"
    }
  }
})`,
          expectedError: "requirePartitionFilter has been declared twice"
        }
      ].forEach(testParameters => {
        test(testParameters.testName, () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          fs.writeFileSync(
            path.join(projectDir, "workflow_settings.yaml"),
            VALID_WORKFLOW_SETTINGS_YAML
          );
          fs.mkdirSync(path.join(projectDir, "definitions"));
          fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
          fs.writeFileSync(
            path.join(projectDir, `definitions/file.js`),
            testParameters.fileContents
          );

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          expect(
            result.compile.compiledGraph.graphErrors.compilationErrors.map(
              compilationError => compilationError.message
            )
          ).deep.equals([testParameters.expectedError]);
        });
      });
    });

    suite(`legacy publish().type() can still be called`, () => {
      ["table", "incremental", "view"].forEach(fromType => {
        ["table", "incremental", "view"].forEach(toType => {
          test(`from type ${fromType} to ${toType}`, () => {
            const projectDir = tmpDirFixture.createNewTmpDir();
            fs.writeFileSync(
              path.join(projectDir, "workflow_settings.yaml"),
              dumpYaml(
                dataform.WorkflowSettings.create(
                  WorkflowSettingsTemplates.bigqueryWithDefaultProjectAndDataset
                )
              )
            );
            fs.mkdirSync(path.join(projectDir, "definitions"));
            fs.writeFileSync(
              path.join(projectDir, "definitions/publish.js"),
              `
publish("name", {type: "${fromType}", schema: "schemaOverride"}).type("${toType}")`
            );

            const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

            expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
            const tables = asPlainObject(result.compile.compiledGraph.tables);
            expect(tables.length).equals(1);
            expect(tables[0].type).equals(toType);
            expect(tables[0].enumType).equals(toType.toUpperCase());

            // Config options are carried over to the new table type, where possible.
            expect(tables[0].target.schema).equals("schemaOverride");
          });
        });
      });
    });
  });
});
