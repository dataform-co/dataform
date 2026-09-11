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

suite("Iceberg table options", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  const setupFiles = (
    projectDir: string,
    filename: string,
    fileContents: string,
    wsContent: string = VALID_WORKFLOW_SETTINGS_YAML
  ) => {
    writeWorkflowSettingsFile(projectDir, wsContent);
    writeDefinitionFile(projectDir, filename, fileContents);
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
        name: "table1",
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
        target: { name: "table1", schema: "dataset1", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "projects/gcp/locations/us/connections/conn-id",
          storageUri: "gs://my-bucket/my-root/my-subpath"
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "with all values provided and dot form connection",
      configBlock: `
        name: "table2",
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
        target: { name: "table2", schema: "dataset2", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/my-root/my-subpath"
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "defaults to `_dataform` for tableFolderRoot",
      configBlock: `
        name: "table3",
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
        target: { name: "table3", schema: "dataset3", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/_dataform/my-subpath"
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName:
        "defaults to dataset and name for tableFolderSubpath with dataset and table name provided",
      configBlock: `
        name: "my-table",
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
        target: { name: "my-table", schema: "my-dataset", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/my-root/my-dataset/my-table"
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName:
        "defaults to dataset and name for tableFolderSubpath with dataset from workflow settings",
      configBlock: `
        name: "my-table",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
          }
        }`,
      expected: {
        target: { name: "my-table", schema: "defaultDataset", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/my-root/defaultDataset/my-table"
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "defaults to PARQUET when file format is not set",
      configBlock: `
        name: "table6",
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
        target: { name: "table6", schema: "dataset6", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "projects/gcp/locations/us/connections/conn-id",
          storageUri: "gs://my-bucket/my-root/my-subpath"
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "defaults to DEFAULT connection",
      configBlock: `
        name: "table7",
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
        target: { name: "table7", schema: "dataset7", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "DEFAULT",
          storageUri: "gs://my-bucket/my-root/my-subpath"
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "defaults to PARQUET when file format is empty",
      configBlock: `
        name: "table6",
        dataset: "dataset6",
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
        target: { name: "table6", schema: "dataset6", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "projects/gcp/locations/us/connections/conn-id",
          storageUri: "gs://my-bucket/my-root/my-subpath"
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "invalid connection format",
      configBlock: `
        name: "table8",
        dataset: "dataset8",
        bigquery: {
          iceberg: {
            connection: "invalid",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
      expectError:
        "The connection must be in the format `{project}.{location}.{connection_id}` or `projects/{project}/locations/{location}/connections/{connection_id}`, or be set to `DEFAULT`.",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "invalid file format",
      configBlock: `
        bigquery: {
          iceberg: {
            fileFormat: "AVRO",
            bucketName: "my-bucket",
          }
        }`,
      expectError: 'Unexpected file format; only "PARQUET" is allowed, got "AVRO".',
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "bucketName not defined",
      configBlock: `
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "projects/gcp/locations/us/connections/conn-id",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
      expectError:
        "When defining an Iceberg table, bucket name must be defined in workflow_settings.yaml or the config block.",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "with Iceberg options and other BigQuery options",
      configBlock: `
        name: "iceberg_mixed",
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
          },
        }`,
      expected: {
        target: { name: "iceberg_mixed", schema: "mixed_dataset", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/my-root/my-subpath",
          partitionBy: "partition_col",
          clusterBy: ["cluster_col1", "cluster_col2"],
          labels: { env: "test", type: "iceberg" },
          additionalOptions: { key1: "val1", key2: "val2" }
        }
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML
    },
    {
      testName: "uses defaultBucketName from workflow_settings.yaml",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
      configBlock: `
        name: "table_ws_bucket",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
      expected: {
        target: { name: "table_ws_bucket", schema: "dataset_ws", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://ws-default-bucket/my-root/my-subpath"
        }
      },
      expectError: false
    },
    {
      testName: "uses defaultTableFolderRoot from workflow_settings.yaml",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
      configBlock: `
        name: "table_ws_root",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            tableFolderSubpath: "my-subpath",
          }
        }`,
      expected: {
        target: { name: "table_ws_root", schema: "dataset_ws", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/ws-default-root/my-subpath"
        }
      },
      expectError: false
    },
    {
      testName: "uses defaultTableFolderSubpath from workflow_settings.yaml",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
      configBlock: `
        name: "table_ws_sub",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            connection: "gcp.us.conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
          }
        }`,
      expected: {
        target: { name: "table_ws_sub", schema: "dataset_ws", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/my-root/ws-default-sub"
        }
      },
      expectError: false
    },
    {
      testName: "uses default connection from workflow_settings.yaml",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
      configBlock: `
        name: "table_ws_sub",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
            bucketName: "my-bucket",
            tableFolderRoot: "my-root",
            tableFolderSubpath: "my-subpath",
          }
        }`,
      expected: {
        target: { name: "table_ws_sub", schema: "dataset_ws", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "ws.default.connection",
          storageUri: "gs://my-bucket/my-root/my-subpath"
        }
      },
      expectError: false
    },
    {
      testName: "uses all Iceberg defaults from workflow_settings.yaml",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
      configBlock: `
        name: "table_ws_all",
        dataset: "dataset_ws",
        bigquery: {
          iceberg: {
            fileFormat: "PARQUET",
          }
        }`,
      expected: {
        target: { name: "table_ws_all", schema: "dataset_ws", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "ws.default.connection",
          storageUri: "gs://ws-default-bucket/ws-default-root/ws-default-sub"
        }
      },
      expectError: false
    },
    {
      testName: "config values override workspace defaults for Iceberg paths",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_ICEBERG_DEFAULTS,
      configBlock: `
        name: "table_override",
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
        target: { name: "table_override", schema: "dataset_ovr", database: "project" },
        bigquery: {
          tableFormat: "ICEBERG",
          fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://config-bucket/config-root/config-sub"
        }
      },
      expectError: false
    }
  ];

  testCases.forEach((testCase, index) => {
    const fileName = `iceberg_test_${index}`;
    const tableConfig = `{
        type: "table",
        database: "project",
        ${testCase.configBlock}
    }`;

    const paramsToTest = [
      {
        filename: `${fileName}.sqlx`,
        fileContents: `config ${tableConfig}\nSELECT 1`
      },
      {
        filename: `${fileName}.js`,
        fileContents: `publish("${fileName}", ${tableConfig}).query(ctx => "SELECT 1")`
      }
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
          expect(compiledTable.target.name).equals(testCase.expected!.target.name);
          expect(compiledTable.target.schema).equals(testCase.expected!.target.schema);
          expect(compiledTable.target.database).equals(testCase.expected!.target.database);

          // Compare the entire bigquery object
          expect(asPlainObject(compiledTable.bigquery)).deep.equals(
            asPlainObject(testCase.expected!.bigquery)
          );
        }
      });
    });
  });
});
