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

suite("table", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("action configs", () => {
    test(`tables can be loaded`, () => {
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
    filename: action.sql`
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
            query: "SELECT 1",
            type: "table",
            enumType: "TABLE",
            disabled: false
          }
        ])
      );
    });
  });

  suite("sqlx and JS API config options", () => {
    const tableConfig = `{
    type: "table",
    name: "name",
    schema: "dataset",
    database: "project",
    dependencies: ["operation"],
    tags: ["tag1", "tag2"],
    disabled: true,
    description: "description",
    ${exampleActionDescriptor.inputSqlxConfigBlock}
    bigquery: {
        partitionBy: "partitionBy",
        partitionExpirationDays: 1,
        requirePartitionFilter: true,
        clusterBy: ["clusterBy"],
        labels: {"key": "val"},
        additionalOptions: {
        option1Key: "option1",
        option2Key: "option2",
        }
    },
    ${exampleBuiltInAssertions.inputAssertionBlock}
    dependOnDependencyAssertions: true,
    hermetic: true,
    metadata: {
        overview: "table overview",
        customAttributes: {
            fields: {
                priority: { stringValue: "high" }
	    }
        }
    }
}`;

    [
      {
        filename: "table.sqlx",
        fileContents: `
config ${tableConfig}
SELECT 1`
      },
      {
        filename: "table.js",
        fileContents: `publish("name", ${tableConfig}).query(ctx => \`\n\nSELECT 1\`)`
      }
    ].forEach(testParameters => {
      test(`for tables configured in a ${testParameters.filename} file`, () => {
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
            type: "table",
            disabled: true,
            hermeticity: "HERMETIC",
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
              requirePartitionFilter: true
            },
            tags: ["tag1", "tag2"],
            dependencyTargets: [
              {
                database: "defaultProject",
                schema: "defaultDataset",
                name: "operation"
              }
            ],
            enumType: "TABLE",
            fileName: `definitions/${testParameters.filename}`,
            query: "\n\nSELECT 1",
            actionDescriptor: {
              ...exampleActionDescriptor.outputActionDescriptor,
              // sqlxConfig.bigquery.labels are placed as bigqueryLabels.
              bigqueryLabels: {
                key: "val"
              },
              metadata: {
                overview: "table overview",

                customAttributes: {
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
- table:
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
    description: description
    partitionBy: partitionBy
    partitionExpirationDays: 1
    requirePartitionFilter: true
    clusterBy:
    - clusterBy
    labels:
        key: val
    additionalOptions:
        option1Key: option1
        option2Key: option2
    dependOnDependencyAssertions: true
    hermetic: true
${exampleBuiltInAssertionsAsYaml.inputActionConfigBlock}
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
        type: "table",
        disabled: true,
        hermeticity: "HERMETIC",
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
          requirePartitionFilter: true
        },
        tags: ["tag1", "tag2"],
        dependencyTargets: [
          {
            database: "defaultProject",
            schema: "defaultDataset",
            name: "operation"
          }
        ],
        enumType: "TABLE",
        fileName: "definitions/filename.sql",
        query: "SELECT 1",
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

  suite("Iceberg table options", () => {
    const setupFiles = (
      projectDir: string,
      filename: string,
      fileContents: string,
      wsContent: string = VALID_WORKFLOW_SETTINGS_YAML
    ) => {
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
          target: {name: "table1", schema: "dataset1", database: "project"},
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
          target: {name: "table2", schema: "dataset2", database: "project"},
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
          target: {name: "table3", schema: "dataset3", database: "project"},
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
          target: {name: "my-table", schema: "my-dataset", database: "project"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/my-root/my-dataset/my-table",
        },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
      },
      {
        testName: "defaults to dataset and name for tableFolderSubpath with dataset from workflow settings",
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
          target: {name: "my-table", schema: "defaultDataset", database: "project"},
          bigquery: {
            tableFormat: "ICEBERG",
            fileFormat: "PARQUET",
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/my-root/defaultDataset/my-table",
        },
        },
        expectError: false,
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
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
          target: {name: "table6", schema: "dataset6", database: "project"},
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
          target: {name: "table7", schema: "dataset7", database: "project"},
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
          target: {name: "table6", schema: "dataset6", database: "project"},
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
        expectError: "The connection must be in the format `{project}.{location}.{connection_id}` or `projects/{project}/locations/{location}/connections/{connection_id}`, or be set to `DEFAULT`.",
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
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
        expectError: "Unexpected file format; only \"PARQUET\" is allowed, got \"AVRO\".",
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
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
        expectError: "When defining an Iceberg table, bucket name must be defined in workflow_settings.yaml or the config block.",
        wsContent: VALID_WORKFLOW_SETTINGS_YAML,
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
          target: {name: "iceberg_mixed", schema: "mixed_dataset", database: "project"},
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
            storageUri: "gs://ws-default-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
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
            storageUri: "gs://my-bucket/ws-default-root/my-subpath",
          },
        },
        expectError: false,
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
            storageUri: "gs://my-bucket/my-root/ws-default-sub",
          },
        },
        expectError: false,
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
            storageUri: "gs://my-bucket/my-root/my-subpath",
          },
        },
        expectError: false,
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
            storageUri: "gs://ws-default-bucket/ws-default-root/ws-default-sub",
          },
        },
        expectError: false,
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
            storageUri: "gs://config-bucket/config-root/config-sub",
          },
        },
        expectError: false,
      },
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
          fileContents: `config ${tableConfig}\nSELECT 1`,
        },
        {
          filename: `${fileName}.js`,
          fileContents: `publish("${fileName}", ${tableConfig}).query(ctx => "SELECT 1")`,
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
});
