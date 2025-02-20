// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import {
  exampleActionDescriptor,
  exampleBuiltInAssertions,
  exampleBuiltInAssertionsAsYaml
} from "df/core/actions/index_test";
import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML
} from "df/testing/run_core";

suite("incremental table", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
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
            hermeticity: "NON_HERMETIC",
            onSchemaChange: "IGNORE",
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
  });

  suite("sqlx and JS API config options", () => {
    const incrementalTableConfig = `{
    type: "incremental",
    name: "name",
    schema: "dataset",
    database: "project",
    dependencies: ["operation"],
    tags: ["tag1", "tag2"],
    disabled: true,
    protected: false,
    uniqueKey: ["key1", "key2"],
    description: "description",
    ${exampleActionDescriptor.inputSqlxConfigBlock}
    bigquery: {
    partitionBy: "partitionBy",
    partitionExpirationDays: 1,
    requirePartitionFilter: true,
    updatePartitionFilter: "updatePartitionFilter",
    clusterBy: ["clusterBy"],
    labels: {"key": "val"},
    additionalOptions: {
        option1Key: "option1",
        option2Key: "option2",
    }
    },
    dependOnDependencyAssertions: true,
    ${exampleBuiltInAssertions.inputAssertionBlock}
    hermetic: true,
    onSchemaChange: "SYNCHRONIZE",
}
`;
    [
      {
        filename: "incremental.sqlx",
        fileContents: `
config ${incrementalTableConfig}
SELECT 1`
      },
      {
        filename: "incremental.js",
        fileContents: `publish("name", ${incrementalTableConfig}).query(ctx => \`\n\n\nSELECT 1\`)`
      }
    ].forEach(testParameters => {
      test(`for incremental tables configured in a ${testParameters.filename} file`, () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          VALID_WORKFLOW_SETTINGS_YAML
        );
        fs.mkdirSync(path.join(projectDir, "definitions"));
        fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
        fs.writeFileSync(
          path.join(projectDir, `definitions/${testParameters.filename}`),
          testParameters.fileContents
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
            protected: false,
            hermeticity: "HERMETIC",
            onSchemaChange: "SYNCHRONIZE",
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
            fileName: `definitions/${testParameters.filename}`,
            query: "\n\n\nSELECT 1",
            incrementalQuery: "\n\n\nSELECT 1",
            actionDescriptor: {
              ...exampleActionDescriptor.outputActionDescriptor,
              // sqlxConfig.bigquery.labels are placed as bigqueryLabels.
              bigqueryLabels: {
                key: "val"
              }
            }
          }
        ]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
          exampleBuiltInAssertions.outputAssertions(testParameters.filename)
        );
      });
    });
  });

  test("action config options", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), VALID_WORKFLOW_SETTINGS_YAML);
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
    fs.writeFileSync(path.join(projectDir, "definitions/filename.sql"), "SELECT 1");
    fs.writeFileSync(
      path.join(projectDir, "definitions/actions.yaml"),
      `
actions:
- incrementalTable:
    name: name
    dataset: dataset
    project: project
    dependencyTargets:
    - name: operation
    filename: filename.sql
    tags:
    - tag1
    - tag2
    disabled: true
    protected: true
    uniqueKey:
    - key1
    - key2
    description: description
    partitionBy: partitionBy
    partitionExpirationDays: 1
    requirePartitionFilter: true
    updatePartitionFilter: "updatePartitionFilter"
    clusterBy:
    - clusterBy
    labels:
        key: val
    additionalOptions:
        option1Key: option1
        option2Key: option2
    dependOnDependencyAssertions: true
    ${exampleBuiltInAssertionsAsYaml.inputActionConfigBlock}
    hermetic: true
    onSchemaChange: FAIL
  `
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
        protected: true,
        hermeticity: "HERMETIC",
        onSchemaChange: "FAIL",
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
        fileName: "definitions/filename.sql",
        query: "SELECT 1",
        incrementalQuery: "SELECT 1",
        actionDescriptor: {
          bigqueryLabels: {
            key: "val"
          },
          description: "description"
        }
      }
    ]);
    expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
      exampleBuiltInAssertionsAsYaml.outputAssertions
    );
  });
});
