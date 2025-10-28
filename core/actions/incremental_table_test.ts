// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import {
  exampleActionDescriptor,
  exampleBuiltInAssertions,
  exampleBuiltInAssertionsAsYaml
} from "df/core/actions/index_test";
import {dataform} from "df/protos/ts";
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
    metadata: {
	overview: "incremental table overview",
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
              },
              metadata: {
                overview: "incremental table overview"
              }
            }
          }
        ]);
        expect(asPlainObject(result.compile.compiledGraph.assertions)).deep.equals(
          exampleBuiltInAssertions.outputAssertions(testParameters.filename)
        );
      });
    });

    test("sqlx minimal config", () => {
      const minimalIncrementalTableName = "minimal_incremental";
      const minimalIncrementalTableContent = `
config {type: "incremental"}

SELECT 1`;
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, "definitions/operation.sqlx"), "SELECT 1");
      fs.writeFileSync(
        path.join(projectDir, `definitions/${minimalIncrementalTableName}.sqlx`),
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
        }
      ]);
    })
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
        option1Key: "option1"
        option2Key: "option2"
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

  suite("Iceberg incremental table options", () => {
    const setupFiles = (projectDir: string, filename: string, fileContents: string, wsContent: string) => {
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        wsContent
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, `definitions/${filename}`), fileContents);
    };

    const CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS = `
defaultProject: "defaultProject"
defaultDataset: "defaultDataset"
defaultLocation: "us-central1"
defaultIcebergConfig:
  bucketName: "ws-default-bucket"
  tableFolderRoot: "ws-default-root"
  tableFolderSubpath: "ws-default-sub"
  connection: "ws.default.connection"
`;

    const testCases = [
      {
        testName: "with all values provided and resource form connection",
        configBlock: `
        type: "incremental",
        name: "incremental_table1",
        dataset: "dataset1",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "projects/gcp/locations/us/connections/conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: {name: "incremental_table1", schema: "dataset1", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "projects/gcp/locations/us/connections/conn-id",
            storageUri: "gs://my-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "with all values provided and dot form connection",
        configBlock: `
        type: "incremental",
        name: "incremental_table2",
        dataset: "dataset2",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: {name: "incremental_table2", schema: "dataset2", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://my-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "defaults to \`_dataform\` for tableFolderRoot",
        configBlock: `
        type: "incremental",
        name: "incremental_table3",
        dataset: "dataset3",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: {name: "incremental_table3", schema: "dataset3", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://my-bucket/_dataform/my-subpath",
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "defaults to dataset and name for tableFolderSubpath with dataset and table name provided",
        configBlock: `
        type: "incremental",
        name: "my-incremental",
        dataset: "my-dataset",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
          }
        }`,
        expected: {
          target: {name: "my-incremental", schema: "my-dataset", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://my-bucket/my-root/my-dataset/my-incremental",
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "defaults to dataset and name for tableFolderSubpath with dataset from workflow settings",
        configBlock: `
        type: "incremental",
        name: "my-incremental",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
          }
        }`,
        expected: {
          target: {name: "my-incremental", schema: "defaultDataset", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://my-bucket/my-root/defaultDataset/my-incremental",
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "defaults to PARQUET when file format is not set",
        configBlock: `
        type: "incremental",
        name: "incremental_table6",
        dataset: "dataset6",
        bigquery: {
          iceberg: {
            connection: "projects/gcp/locations/us/connections/conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: {name: "incremental_table6", schema: "dataset6", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "projects/gcp/locations/us/connections/conn-id",
            storageUri: "gs://my-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "defaults to DEFAULT connection",
        configBlock: `
        type: "incremental",
        name: "incremental_table7",
        dataset: "dataset7",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: {name: "incremental_table7", schema: "dataset7", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "DEFAULT",
            storageUri: "gs://my-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "defaults to PARQUET when file format is empty",
        configBlock: `
        type: "incremental",
        name: "incremental_table8",
        dataset: "dataset8",
        bigquery: {
          iceberg: {
            fileFormat: "",
            connection: "projects/gcp/locations/us/connections/conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: {name: "incremental_table8", schema: "dataset8", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "projects/gcp/locations/us/connections/conn-id",
            storageUri: "gs://my-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "invalid connection format",
        configBlock: `
        type: "incremental",
        name: "incremental_table9",
        dataset: "dataset9",
        bigquery: {
          iceberg: {
            connection: "invalid",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expectError: "The connection must be in the format `{project}.{location}.{connection_id}` or `projects/{project}/locations/{location}/connections/{connection_id}`, or be set to `DEFAULT`.",
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "invalid file format",
        configBlock: `
        type: "incremental",
        name: "incremental_table10",
        bigquery: {
          iceberg: {
            fileFormat: "AVRO",
            bucketName: "my-bucket",
          }
        }`,
        expectError: "Unexpected file format; only \"PARQUET\" is allowed, got \"AVRO\".",
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "bucketName not defined",
        configBlock: `
        type: "incremental",
        name: "incremental_table11",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "projects/gcp/locations/us/connections/conn-id",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expectError: "When defining an Iceberg table, bucket name must be defined in workflow_settings.yaml or the config block.",
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "with Iceberg options and other BigQuery options",
        configBlock: `
        type: "incremental",
        name: "iceberg_incremental_mixed",
        dataset: "mixed_dataset",
        bigquery: {
            partitionBy: "partition_col",
            clusterBy: ["cluster_col1", "cluster_col2"],
            labels: {"env": "test", "type": "iceberg"},
            additionalOptions: { "key1": "val1", "key2": "val2" },
            iceberg: {
              fileFormat: "PARQUET",
              connection: "gcp.us.conn-id",
              bucketName: "my-bucket",
              tableFolderRoot: "my-root",
              tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: {name: "iceberg_incremental_mixed", schema: "mixed_dataset", database: "defaultProject"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://my-bucket/my-root/my-subpath",
            partitionBy: "partition_col",
            clusterBy: ["cluster_col1", "cluster_col2"],
            labels: {"env": "test", "type": "iceberg"},
            additionalOptions: {"key1": "val1", "key2": "val2"},
          },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "uses defaultBucketName from workflow_settings.yaml",
        wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
        configBlock: `
        type: "incremental",
        name: "incremental_ws_bucket",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            // bucketName omitted
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: { name: "incremental_ws_bucket", schema: "dataset_ws", database: "defaultProject" },
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://ws-default-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
      },
      {
        testName: "uses defaultTableFolderRoot from workflow_settings.yaml",
        wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
        configBlock: `
        type: "incremental",
        name: "incremental_ws_root",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            // tableFolderRoot omitted
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expected: {
          target: { name: "incremental_ws_root", schema: "dataset_ws", database: "defaultProject" },
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://my-bucket/ws-default-root/my-subpath",
          },
        },
        expectError: false,
      },
      {
        testName: "uses defaultTableFolderSubpath from workflow_settings.yaml",
        wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
        configBlock: `
        type: "incremental",
        name: "incremental_ws_sub",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            // tableFolderSubpath omitted
          }
        }`,
        expected: {
          target: { name: "incremental_ws_sub", schema: "dataset_ws", database: "defaultProject" },
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://my-bucket/my-root/ws-default-sub",
          },
        },
        expectError: false,
      },
      {
        testName: "uses default connection from workflow_settings.yaml",
        wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
        configBlock: `
        type: "incremental",
        name: "incremental_ws_sub",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath"
          }
        }`,
        expected: {
          target: { name: "incremental_ws_sub", schema: "dataset_ws", database: "defaultProject" },
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "ws.default.connection",
            storageUri: "gs://my-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
      },
      {
        testName: "uses all Iceberg defaults from workflow_settings.yaml",
        wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
        configBlock: `
        type: "incremental",
        name: "incremental_ws_all",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
          }
        }`,
        expected: {
          target: { name: "incremental_ws_all", schema: "dataset_ws", database: "defaultProject" },
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "ws.default.connection",
            storageUri: "gs://ws-default-bucket/ws-default-root/ws-default-sub",
          },
        },
        expectError: false,
      },
      {
        testName: "config values override workspace defaults for Iceberg paths",
        wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
        configBlock: `
        type: "incremental",
        name: "incremental_override",
        dataset: "dataset_ovr",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "config-bucket",
            tableFolderRoot: "config-root",
            tableFolderSubpath: "config-sub",
          }
        }`,
        expected: {
          target: { name: "incremental_override", schema: "dataset_ovr", database: "defaultProject" },
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            storageUri: "gs://config-bucket/config-root/config-sub",
          },
        },
        expectError: false,
      },
       {
        testName: "bucketName not defined in config or workspace settings",
        configBlock: `
        type: "incremental",
        name: "incremental_no_bucket",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "projects/gcp/locations/us/connections/conn-id",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
        expectError: "When defining an Iceberg table, bucket name must be defined in workflow_settings.yaml or the config block.",
        wsContent: VALID_WORKFLOW_SETTINGS_YAML, // Ensure no defaults are set here
      },
    ];

    testCases.forEach((testCase, index) => {
      const fileName = `iceberg_incremental_test_${index}`;
      const configBlock = testCase.configBlock;

      const paramsToTest = [
        {
          filename: `${fileName}.sqlx`,
          fileContents: `config { ${configBlock} }\nSELECT 1`,
        },
        {
          filename: `${fileName}.js`,
          fileContents: `publish("${fileName}", { ${configBlock} }).query(ctx => "SELECT 1")`,
        },
      ];

      paramsToTest.forEach(params => {
        test(`${testCase.testName} in ${params.filename}`, () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          setupFiles(projectDir, params.filename, params.fileContents, testCase.wsContent);

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          if (testCase.expectError) {
            expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
            const error = result.compile.compiledGraph.graphErrors.compilationErrors[0];
            expect(error.message).contains(testCase.expectError);
            expect(error.fileName).equals(`definitions/${params.filename}`);
          } else {
            expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
            const compiledTable = result.compile.compiledGraph.tables[0];
            expect(compiledTable.type).equals("incremental");
            expect(compiledTable.enumType).equals(dataform.TableType.INCREMENTAL);
            expect(compiledTable.target.name).equals(testCase.expected!.target.name);
            expect(compiledTable.target.schema).equals(testCase.expected!.target.schema);
            expect(compiledTable.target.database).equals(testCase.expected!.target.database);

            // Compare the bigquery object
            expect(asPlainObject(compiledTable.bigquery)).deep.equals(
              asPlainObject(testCase.expected!.bigquery)
            );
          }
        });
      });
    });
  });
});
