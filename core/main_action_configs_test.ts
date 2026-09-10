import { expect } from "chai";

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
    filename: doesnotexist.sql`
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "Cannot find module 'definitions/doesnotexist.sql'"
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
    materialized: true`
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      `Unexpected property "materialized", or property value type of "boolean" is incorrect. See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-ActionConfigs for allowed properties.`
    );
  });

  test(`fails when empty objects are given`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "actions.yaml",
      `
actions:`
    );

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      `Unexpected empty value for "actions". See https://dataform-co.github.io/dataform/docs/configs-reference#dataform-ActionConfigs for allowed properties.`
    );
  });

  suite(`.sqlx filenames in actions.yaml are rejected with a clear error`, () => {
    [
      {
        actionType: "table",
        yamlBody: `
actions:
- table:
    filename: table.sqlx`
      },
      {
        actionType: "view",
        yamlBody: `
actions:
- view:
    filename: view.sqlx`
      },
      {
        actionType: "incrementalTable",
        yamlBody: `
actions:
- incrementalTable:
    filename: incremental.sqlx`
      },
      {
        actionType: "assertion",
        yamlBody: `
actions:
- assertion:
    filename: assertion.sqlx`
      },
      {
        actionType: "operation",
        yamlBody: `
actions:
- operation:
    filename: operation.sqlx`
      },
      {
        actionType: "declaration",
        yamlBody: `
actions:
- declaration:
    name: declaration
    filename: declaration.sqlx`
      },
      {
        actionType: "notebook",
        yamlBody: `
actions:
- notebook:
    filename: notebook.sqlx`
      }
    ].forEach(({ actionType, yamlBody }) => {
      test(`for action type "${actionType}"`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
        writeDefinitionFile(projectDir, "actions.yaml", yamlBody);

        const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

        const errorMessages = result.compile.compiledGraph.graphErrors.compilationErrors.map(
          ({ message }) => message
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
    filename: table.SQLX`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(
        result.compile.compiledGraph.graphErrors.compilationErrors.map(({ message }) => message)
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
    filename: prep.dp.sqlx`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      const errorMessages = result.compile.compiledGraph.graphErrors.compilationErrors.map(
        ({ message }) => message
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
    filename: utf8characters:私🙂 and some spaces.sql`
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
      project: defaultProject`
    );
    ["table.sql", "incrementalTable.sql", "view.sql", "operation.sql"].forEach(filename => {
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
    filename: operation.sql`
    );
    writeDefinitionFile(projectDir, "operation.sql", "SELECT 1");
    writeDefinitionFile(projectDir, "notebook.ipynb", EMPTY_NOTEBOOK_CONTENTS);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
  });
});
