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

const EMPTY_NOTEBOOK_CONTENTS = '{ "cells": [] }';

suite("notebook", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

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
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), EMPTY_NOTEBOOK_CONTENTS);

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
  outputBucket: gs://some-bucket
  runtimeTemplateName: projects/test-project/locations/us-central1/notebookRuntimeTemplates/test-template
  `);
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), EMPTY_NOTEBOOK_CONTENTS);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals({
      defaultDatabase: "dataform",
      defaultLocation: "US",
      defaultNotebookRuntimeOptions: {
        outputBucket: "gs://some-bucket",
        runtimeTemplateName:
          "projects/test-project/locations/us-central1/notebookRuntimeTemplates/test-template"
      },
      warehouse: "bigquery"
    });
  });

  suite("sqlx and JS API config options", () => {
    test(`for notebooks`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
      fs.writeFileSync(
        path.join(projectDir, "definitions/filename.ipynb"),
        EMPTY_NOTEBOOK_CONTENTS
      );
      fs.writeFileSync(
        path.join(projectDir, "definitions/notebook.js"),
        `
notebook({
name: "name",
location: "location",
project: "project",
dependencyTargets: [{
  name: "operation",
}],
filename: "filename.ipynb",
tags: ["tagA", "tagB"],
disabled: true,
description: "description",
dependOnDependencyAssertions: true
})`
      );

      const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

      expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
      expect(asPlainObject(result.compile.compiledGraph.notebooks)).deep.equals(
        asPlainObject([
          {
            target: {
              database: "project",
              schema: "location",
              name: "name"
            },
            canonicalTarget: {
              database: "project",
              schema: "location",
              name: "name"
            },
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "operation"
              }
            ],
            disabled: true,
            fileName: "definitions/filename.ipynb",
            tags: ["tagA", "tagB"],
            notebookContents: `{"cells":[]}`
          }
        ])
      );
    });
  });

  test(`action config options`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(path.join(projectDir, "definitions"));

    fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
    fs.writeFileSync(path.join(projectDir, "definitions/filename.ipynb"), EMPTY_NOTEBOOK_CONTENTS);
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- notebook:
    name: name
    location: location
    project: project
    dependencyTargets:
    - name: operation
    filename: filename.ipynb
    tags:
    - tagA
    - tagB
    disabled: true
    description: description
    dependOnDependencyAssertions: true`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.notebooks)).deep.equals(
      asPlainObject([
        {
          target: {
            database: "project",
            schema: "location",
            name: "name"
          },
          canonicalTarget: {
            database: "project",
            schema: "location",
            name: "name"
          },
          dependencyTargets: [
            {
              database: "defaultProject",
              schema: "defaultDataset",
              name: "operation"
            }
          ],
          disabled: true,
          fileName: "definitions/filename.ipynb",
          tags: ["tagA", "tagB"],
          notebookContents: `{"cells":[]}`
        }
      ])
    );
  });
});
