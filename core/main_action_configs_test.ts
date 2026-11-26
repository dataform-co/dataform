import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import {
  asPlainObject,
  suite,
  test,
  writeDefinitionFile,
  writeWorkflowSettingsFile,
} from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML,
} from "df/testing/run_core";

const EMPTY_NOTEBOOK_CONTENTS = '{ "cells": [] }';

suite("action configs", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  test(`fails when file is not found`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- operation:
    filename: doesnotexist.sql`,
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "Cannot find module 'definitions/doesnotexist.sql'",
    );
  });

  test(`fails when properties belonging to other action config types are populated for an action config`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- table:
    filename: action.sql
    materialized: true`,
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      `Unexpected property "materialized", or property value type of "boolean" is incorrect. See https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfigs for allowed properties.`,
    );
  });

  test(`fails when empty objects are given`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:`,
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      `Unexpected empty value for "actions". See https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfigs for allowed properties.`,
    );
  });

  suite(`.sqlx filenames in actions.yaml are rejected with a clear error`, () => {
    [
      {
        actionType: "table",
        yamlBody: `
actions:
- table:
    filename: table.sqlx`,
      },
      {
        actionType: "view",
        yamlBody: `
actions:
- view:
    filename: view.sqlx`,
      },
      {
        actionType: "incrementalTable",
        yamlBody: `
actions:
- incrementalTable:
    filename: incremental.sqlx`,
      },
      {
        actionType: "assertion",
        yamlBody: `
actions:
- assertion:
    filename: assertion.sqlx`,
      },
      {
        actionType: "operation",
        yamlBody: `
actions:
- operation:
    filename: operation.sqlx`,
      },
      {
        actionType: "declaration",
        yamlBody: `
actions:
- declaration:
    name: declaration
    filename: declaration.sqlx`,
      },
      {
        actionType: "notebook",
        yamlBody: `
actions:
- notebook:
    filename: notebook.sqlx`,
      },
    ].forEach(({ actionType, yamlBody }) => {
      test(`for action type "${actionType}"`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(projectDir, "actions.yaml", yamlBody);

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        const errorMessages = result.compile.compiledGraph.graphErrors.compilationErrors.map(
          ({ message }) => message,
        );
        expect(errorMessages).to.have.lengthOf(1);
        expect(errorMessages[0]).to.include(`Action config "${actionType}" has filename`);
        expect(errorMessages[0]).to.include(".sqlx files cannot be referenced from actions.yaml");
      });
    });

    test(`is case-insensitive`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- table:
    filename: table.SQLX`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors.map(({ message }) => message),
      ).to.have.lengthOf(1);
    });

    test(`for data preparations referencing a .dp.sqlx file`, () => {
      // .dp.sqlx data preparations are compiled directly from the definitions/
      // directory, so referencing one from actions.yaml is the same mistake as
      // for any other action type and must produce the same clear error rather
      // than silently loading a malformed, duplicated action.
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- dataPreparation:
    filename: prep.dp.sqlx`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      const errorMessages = result.compile.compiledGraph.graphErrors.compilationErrors.map(
        ({ message }) => message,
      );
      expect(errorMessages).to.have.lengthOf(1);
      expect(errorMessages[0]).to.include(`Action config "dataPreparation" has filename`);
      expect(errorMessages[0]).to.include(".sqlx files cannot be referenced from actions.yaml");
    });
  });

  test(`filenames with non-UTF8 characters are valid`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:
- operation:
    filename: utf8characters:私🙂 and some spaces.sql`,
    );
    writeDefinitionFile(projectDir, "utf8characters:私🙂 and some spaces.sql", "SELECT 1");

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.operations)).deep.equals(
      asPlainObject([
        {
          target: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "utf8characters:私🙂 and some spaces",
          },
          canonicalTarget: {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "utf8characters:私🙂 and some spaces",
          },
          fileName: "definitions/utf8characters:私🙂 and some spaces.sql",
          queries: ["SELECT 1"],
          hermeticity: "NON_HERMETIC",
        },
      ]),
    );
  });

  test(`dependency targets are loaded`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
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
      project: defaultProject`,
    );
    ["table.sql", "incrementalTable.sql", "view.sql", "operation.sql"].forEach((filename) => {
      writeDefinitionFile(projectDir, filename, "SELECT 1");
    });
    writeDefinitionFile(projectDir, "notebook.ipynb", EMPTY_NOTEBOOK_CONTENTS);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
  });

  test(`dependency targets of actions with different types are loaded`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    // The dependency target for depending on a notebook currently hacks around the limitations of
    // the target proto, until proper target support for notebooks is added.
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
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
    filename: operation.sql`,
    );
    writeDefinitionFile(projectDir, "operation.sql", "SELECT 1");
    writeDefinitionFile(projectDir, "notebook.ipynb", EMPTY_NOTEBOOK_CONTENTS);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
  });

  suite("jobLabels with numeric and boolean values are coerced to strings", () => {
    [
      { actionType: "table", filename: "action.sql", content: "SELECT 1" },
      { actionType: "view", filename: "action.sql", content: "SELECT 1" },
      { actionType: "incrementalTable", filename: "action.sql", content: "SELECT 1" },
      { actionType: "assertion", filename: "action.sql", content: "SELECT 1" },
      { actionType: "operation", filename: "action.sql", content: "SELECT 1" },
      {
        actionType: "dataPreparation",
        filename: "action.dp.yaml",
        content: "nodes: []",
      },
    ].forEach(({ actionType, filename, content }) => {
      test(`${actionType} coerces numeric and boolean jobLabels values in actions.yaml`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(
          projectDir,
          "actions.yaml",
          `
actions:
- ${actionType}:
    filename: ${filename}
    jobLabels:
      num_label: 123
      bool_label: true
`,
        );
        writeDefinitionFile(projectDir, filename, content);

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
        const compiledActions = [
          ...result.compile.compiledGraph.tables,
          ...result.compile.compiledGraph.assertions,
          ...result.compile.compiledGraph.operations,
          ...result.compile.compiledGraph.dataPreparations,
        ];
        expect(compiledActions).to.have.lengthOf(1);
        expect(asPlainObject(compiledActions[0]?.actionDescriptor?.jobLabels)).deep.equals({
          num_label: "123",
          bool_label: "true",
        });
      });
    });
  });

  suite("actions.yaml enum and declaration column handling", () => {
    test("compiles table and incrementalTable with iceberg fileFormat PARQUET in actions.yaml", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- table:
    filename: tbl.sql
    iceberg:
      bucketName: my-bucket
      fileFormat: PARQUET
- incrementalTable:
    filename: inc.sql
    iceberg:
      bucketName: my-bucket
      fileFormat: PARQUET`,
      );
      writeDefinitionFile(projectDir, "tbl.sql", "SELECT 1");
      writeDefinitionFile(projectDir, "inc.sql", "SELECT 1");

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "tbl",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "tbl",
            },
            type: "table",
            enumType: "TABLE",
            disabled: false,
            fileName: "definitions/tbl.sql",
            query: "SELECT 1",
            hermeticity: "NON_HERMETIC",
            bigquery: {
              connection: "DEFAULT",
              fileFormat: "PARQUET",
              tableFormat: "ICEBERG",
              storageUri: "gs://my-bucket/_dataform/defaultDataset/tbl",
            },
          },
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "inc",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "inc",
            },
            type: "incremental",
            enumType: "INCREMENTAL",
            disabled: false,
            protected: false,
            fileName: "definitions/inc.sql",
            query: "SELECT 1",
            incrementalQuery: "SELECT 1",
            incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
            onSchemaChange: "IGNORE",
            hermeticity: "NON_HERMETIC",
            bigquery: {
              connection: "DEFAULT",
              fileFormat: "PARQUET",
              tableFormat: "ICEBERG",
              storageUri: "gs://my-bucket/_dataform/defaultDataset/inc",
            },
          },
        ]),
      );
    });

    test("preserves declaration column paths in actions.yaml", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- declaration:
    name: raw_users
    dataset: raw
    columns:
      - path:
          - user_id
        description: Primary user key
      - path:
          - profile
          - email
        description: Nested email`,
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.declarations)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "raw",
              name: "raw_users",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "raw",
              name: "raw_users",
            },
            actionDescriptor: {
              columns: [
                { path: ["user_id"], description: "Primary user key" },
                { path: ["profile", "email"], description: "Nested email" },
              ],
            },
          },
        ]),
      );
    });

    test("transpiles explicitly referenced uppercase .SQL, .IPYNB and .MD files without auto-discovering uppercase definitions", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      fs.ensureDirSync(path.join(projectDir, "includes"));
      fs.writeFileSync(
        path.join(projectDir, "includes/ignored_helper.JS"),
        "module.exports = { val: 'ignored' };\n",
      );
      writeDefinitionFile(projectDir, "ignored_table.SQLX", `config { type: "table" }\nSELECT 999`);
      writeDefinitionFile(
        projectDir,
        "ignored_script.JS",
        `publish("ignored_js").query("SELECT 999");`,
      );
      writeDefinitionFile(
        projectDir,
        "actions.YAML",
        `actions:\n- table:\n    filename: ignored_from_yaml.sql\n`,
      );
      writeDefinitionFile(projectDir, "ignored_from_yaml.sql", "SELECT 999");
      writeDefinitionFile(projectDir, "graph.YAML", "invalid_graph_yaml: true\n");

      writeDefinitionFile(
        projectDir,
        "actions.yaml",
        `
actions:
- table:
    filename: upper_query.SQL
- notebook:
    filename: upper_nb.IPYNB`,
      );
      writeDefinitionFile(
        projectDir,
        "upper_query.SQL",
        `global.__upper_sql_ran_as_js = "RAN";\nSELECT 1 AS id`,
      );
      writeDefinitionFile(
        projectDir,
        "upper_nb.IPYNB",
        JSON.stringify({ cells: [], nbformat: 4, nbformat_minor: 3 }),
      );
      writeDefinitionFile(
        projectDir,
        "README.MD",
        `global.__upper_md_ran_as_js = "RAN";\n# Uppercase docs`,
      );
      writeDefinitionFile(
        projectDir,
        "with_md.sqlx",
        `config { type: "view", description: \`\${getContents("README.MD")}\` }\n` +
          `SELECT '\${global.__upper_sql_ran_as_js || "SAFE"}:\${global.__upper_md_ran_as_js || "SAFE"}:\${typeof ignored_helper}' AS status`,
      );

      // Include uppercase paths explicitly in compileConfig.filePaths (matching CLI and Cloud hosts)
      // because coreExecutionRequestFromPath pre-filters by lowercase SOURCE_EXTENSIONS.
      const request = coreExecutionRequestFromPath(projectDir);
      request.compile.compileConfig.filePaths.push(
        "includes/ignored_helper.JS",
        "definitions/ignored_table.SQLX",
        "definitions/ignored_script.JS",
        "definitions/actions.YAML",
        "definitions/graph.YAML",
      );
      const result = runMainInVm(request);

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(result.compile.compiledGraph.propertyGraphs).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals(
        asPlainObject([
          {
            type: "table",
            enumType: "TABLE",
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "upper_query",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "upper_query",
            },
            fileName: "definitions/upper_query.SQL",
            query: `global.__upper_sql_ran_as_js = "RAN";\nSELECT 1 AS id`,
            disabled: false,
            hermeticity: "NON_HERMETIC",
          },
          {
            type: "view",
            enumType: "VIEW",
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "with_md",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "with_md",
            },
            fileName: "definitions/with_md.sqlx",
            query: "\nSELECT 'SAFE:SAFE:undefined' AS status",
            actionDescriptor: {
              description: `global.__upper_md_ran_as_js = "RAN";\n# Uppercase docs`,
            },
            disabled: false,
            hermeticity: "NON_HERMETIC",
          },
        ]),
      );
      expect(asPlainObject(result.compile.compiledGraph.notebooks)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "upper_nb",
            },
            canonicalTarget: {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "upper_nb",
            },
            fileName: "definitions/upper_nb.IPYNB",
            notebookContents: '{"cells":[],"nbformat":4,"nbformat_minor":3}',
          },
        ]),
      );
    });
  });
});
