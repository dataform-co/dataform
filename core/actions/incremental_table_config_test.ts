import { expect } from "chai";

import { exampleActionDescriptor, exampleBuiltInAssertions } from "df/core/actions/index_test";
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

suite("incremental table sqlx and JS API config options", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

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
    reservation: "reservation",
    onSchemaChange: "SYNCHRONIZE",
    metadata: {
        overview: "incremental table overview",
        extraProperties: {
            fields: {
                priority: { stringValue: "high" }
            }
        }
    },
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
      writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
      writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");
      writeDefinitionFile(projectDir, testParameters.filename, testParameters.fileContents);

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
          incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
          actionDescriptor: {
            ...exampleActionDescriptor.outputActionDescriptor,
            reservation: "reservation",
            // sqlxConfig.bigquery.labels are placed as bigqueryLabels.
            bigqueryLabels: {
              key: "val"
            },
            metadata: {
              overview: "incremental table overview",
              extraProperties: {
                fields: {
                  priority: { stringValue: "high" }
                }
              }
            }
          }
        }
      ]);
      expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
        exampleBuiltInAssertions.outputAssertions(testParameters.filename)
      );
    });
  });

  test("onSchemaChange combined with metadata.extraProperties as a plain object", () => {
    const tableName = "on_schema_change_with_metadata";
    const tableContent = `
config {
  type: "incremental",
  onSchemaChange: "IGNORE",
  metadata: {
    extraProperties: {
      priority: "high"
    }
  }
}

SELECT 1`;
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, `${tableName}.sqlx`, tableContent);

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
      {
        target: {
          database: "defaultProject",
          schema: "defaultDataset",
          name: tableName
        },
        canonicalTarget: {
          database: "defaultProject",
          schema: "defaultDataset",
          name: tableName
        },
        type: "incremental",
        disabled: false,
        protected: false,
        hermeticity: "NON_HERMETIC",
        onSchemaChange: "IGNORE",
        enumType: "INCREMENTAL",
        fileName: `definitions/${tableName}.sqlx`,
        query: "\n\n\nSELECT 1",
        incrementalQuery: "\n\n\nSELECT 1",
        incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED",
        actionDescriptor: {
          metadata: {
            extraProperties: {
              fields: {
                priority: { stringValue: "high" }
              }
            }
          }
        }
      }
    ]);
  });

  test("sqlx minimal config", () => {
    const minimalIncrementalTableName = "minimal_incremental";
    const minimalIncrementalTableContent = `
config {type: "incremental"}

SELECT 1`;
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(projectDir, "operation.sqlx", "SELECT 1");
    writeDefinitionFile(
      projectDir,
      `${minimalIncrementalTableName}.sqlx`,
      minimalIncrementalTableContent
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(asPlainObject(result.compile.compiledGraph.tables)).deep.equals([
      {
        target: {
          database: "defaultProject",
          schema: "defaultDataset",
          name: minimalIncrementalTableName
        },
        canonicalTarget: {
          database: "defaultProject",
          schema: "defaultDataset",
          name: minimalIncrementalTableName
        },
        type: "incremental",
        disabled: false,
        protected: false,
        hermeticity: "NON_HERMETIC",
        onSchemaChange: "IGNORE",
        enumType: "INCREMENTAL",
        fileName: `definitions/${minimalIncrementalTableName}.sqlx`,
        query: "\n\n\nSELECT 1",
        incrementalQuery: "\n\n\nSELECT 1",
        incrementalStrategy: "INCREMENTAL_STRATEGY_UNSPECIFIED"
      }
    ]);
  });

  test("throws an error when incrementalPredicates is not an array", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "invalid_predicates.sqlx",
      `config {
  type: "incremental",
  bigquery: {
    incrementalPredicates: "not_an_array"
  }
}
SELECT 1`
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
    expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
      "incrementalPredicates"
    );
  });
});
