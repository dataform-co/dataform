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

  test(`notebook cell output and metadata are removed`, () => {
    const projectDir = createSimpleNotebookProject();

    fs.writeFileSync(
      path.join(projectDir, "definitions/notebook.ipynb"),
      JSON.stringify({
        metadata: { kernelspec: { name: "python3" }, language_info: { name: "python" } },
        cells: [
          {
            cell_type: "markdown",
            source: ["# Some title"],
            outputs: ["something"],
            metadata: { id: "cell-1" }
          },
          {
            cell_type: "code",
            source: ["print('hi')"],
            outputs: ["hi"],
            execution_count: 5,
            metadata: { scrolled: true }
          },
          {
            cell_type: "raw",
            source: ["print('hi')"],
            metadata: {}
          }
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
            metadata: {},
            cells: [
              {
                cell_type: "markdown",
                source: ["# Some title"],
                outputs: [],
                metadata: {}
              },
              {
                cell_type: "code",
                source: ["print('hi')"],
                outputs: [],
                execution_count: null,
                metadata: {}
              },
              {
                cell_type: "raw",
                source: ["print('hi')"],
                metadata: {}
              }
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
  repositorySnapshotDestination:
    repositorySnapshotUri: gs://some-other-bucket
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
          "projects/test-project/locations/us-central1/notebookRuntimeTemplates/test-template",
        repositorySnapshotDestination: {
          repositorySnapshotUri: "gs://some-other-bucket"
        }
      },
      warehouse: "bigquery"
    });
  });

  test(`notebook default runtime options snapshot destination defaults to output bucket`, () => {
    const projectDir = createSimpleNotebookProject(`
defaultProject: dataform
defaultLocation: US
defaultNotebookRuntimeOptions:
  outputBucket: gs://some-bucket
  runtimeTemplateName: projects/test-project/locations/us-central1/notebookRuntimeTemplates/test-template
  repositorySnapshotDestination: {}
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
          "projects/test-project/locations/us-central1/notebookRuntimeTemplates/test-template",
        repositorySnapshotDestination: {
          repositorySnapshotUri: "gs://some-bucket"
        }
      },
      warehouse: "bigquery"
    });
  });

  test(`notebook default runtime options throw for snapshot destination with no uri or output bucket`, () => {
    const projectDir = createSimpleNotebookProject(`
defaultProject: dataform
defaultLocation: US
defaultNotebookRuntimeOptions:
  runtimeTemplateName: projects/test-project/locations/us-central1/notebookRuntimeTemplates/test-template
  repositorySnapshotDestination: {}
  `);
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), EMPTY_NOTEBOOK_CONTENTS);

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "Invalid repository_snapshot_destination: either repository_snapshot_uri or output_bucket has to be defined"
    );
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

  test(`notebook default managed spark execution options are loaded`, () => {
    const projectDir = createSimpleNotebookProject(`
defaultProject: dataform
defaultLocation: US
defaultManagedSparkExecutionOptions:
  stagingBucketUri: gs://some-bucket
  `);
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), EMPTY_NOTEBOOK_CONTENTS);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals({
      defaultDatabase: "dataform",
      defaultLocation: "US",
      defaultManagedSparkExecutionOptions: {
        stagingBucketUri: "gs://some-bucket"
      },
      warehouse: "bigquery"
    });
  });

  test(`notebook default managed spark execution options throw if stagingBucketUri is missing`, () => {
    const projectDir = createSimpleNotebookProject(`
defaultProject: dataform
defaultLocation: US
defaultManagedSparkExecutionOptions: {}
  `);
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), EMPTY_NOTEBOOK_CONTENTS);

    expect(() => runMainInVm(coreExecutionRequestFromPath(projectDir))).to.throw(
      "Invalid default_managed_spark_execution_options: staging_bucket_uri is required"
    );
  });

  test(`notebook action executionEngine is propagated`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
defaultManagedSparkExecutionOptions:
  stagingBucketUri: gs://some-bucket
`
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), EMPTY_NOTEBOOK_CONTENTS);
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- notebook:
    filename: notebook.ipynb
    executionEngine: MANAGED_SPARK`
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
          notebookContents: JSON.stringify({ cells: [] }),
          executionEngine: "MANAGED_SPARK"
        }
      ])
    );
  });

  test(`notebook action executionEngine MANAGED_SPARK throws if defaultManagedSparkExecutionOptions is not defined at project level`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: dataform
defaultLocation: US
`
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), EMPTY_NOTEBOOK_CONTENTS);
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- notebook:
    filename: notebook.ipynb
    executionEngine: MANAGED_SPARK`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
    expect(result.compile.compiledGraph.graphErrors.compilationErrors.map(err => err.message)).to.include(
      "defaultManagedSparkExecutionOptions must be defined at the project level when execution engine is MANAGED_SPARK"
    );
  });

  test(`notebook action executionEngine COLAB compiles successfully without project defaults`, () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: dataform
defaultLocation: US
`
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), EMPTY_NOTEBOOK_CONTENTS);
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- notebook:
    filename: notebook.ipynb
    executionEngine: COLAB`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
  });
});

