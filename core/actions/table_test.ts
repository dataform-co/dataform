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
import {dataform} from "df/protos/ts";

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
    hermetic: true
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
    const setupFiles = (projectDir: string, filename: string, fileContents: string) => {
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      fs.mkdirSync(path.join(projectDir, "definitions"));
      fs.writeFileSync(path.join(projectDir, `definitions/${filename}`), fileContents);
    };

    const testCases = [
      {
        testName: "with provided storage_uri",
        icebergConfigBlock: `
        fileFormat: "PARQUET",
        iceberg: {
            connection: "projects/gcp/locations/us/connections/conn-id",
            storageUri: "gs://my-bucket/table-data",
        }`,
        expected: {
          fileFormat: dataform.FileFormat.PARQUET,
          connection: "projects/gcp/locations/us/connections/conn-id",
          storageUri: "gs://my-bucket/table-data",
        },
        expectError: false,
      },
      {
        testName: "with constructed storage_uri",
        icebergConfigBlock: `
        fileFormat: "PARQUET",
        iceberg: {
            connection: "projects/gcp/locations/us/connections/conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "root",
            tableFolderSubpath: "subpath",
        }`,
        expected: {
          fileFormat: dataform.FileFormat.PARQUET,
          connection: "projects/gcp/locations/us/connections/conn-id",
          storageUri: "gs://my-bucket/root/subpath",
        },
        expectError: false,
      },
      {
        testName: "takes the provided storage_uri and does not construct",
        icebergConfigBlock: `
        fileFormat: "PARQUET",
        iceberg: {
            connection: "projects/gcp/locations/us/connections/conn-id",
            bucketName: "my-bucket",
            tableFolderRoot: "root",
            tableFolderSubpath: "subpath",
            storageUri: "gs://provided/storage/uri",
        }`,
        expected: {
          fileFormat: dataform.FileFormat.PARQUET,
          connection: "projects/gcp/locations/us/connections/conn-id",
          storageUri: "gs://provided/storage/uri",
        },
        expectError: false,
      },
      {
        testName: "with dot form connection",
        icebergConfigBlock: `
        fileFormat: "PARQUET",
        iceberg: {
            connection: "gcp.us.conn-id",
            storageUri: "gs://my-bucket/root/subpath",
        }`,
        expected: {
          fileFormat: dataform.FileFormat.PARQUET,
          connection: "gcp.us.conn-id",
          storageUri: "gs://my-bucket/root/subpath",
        },
        expectError: false,
      },
      {
        testName: "defaults to PARQUET file format",
        icebergConfigBlock: `
        iceberg: {
            connection: "projects/gcp/locations/us/connections/conn-id",
            storageUri: "gs://my-bucket/root/subpath",
        }`,
        expected: {
          fileFormat: dataform.FileFormat.PARQUET,
          connection: "projects/gcp/locations/us/connections/conn-id",
          storageUri: "gs://my-bucket/root/subpath",
        },
        expectError: false,
      },
      {
        testName: "defaults to DEFAULT connection",
        icebergConfigBlock: `
        fileFormat: "PARQUET",
        iceberg: {
            storageUri: "gs://my-bucket/root/subpath",
        }`,
        expected: {
          fileFormat: dataform.FileFormat.PARQUET,
          connection: "DEFAULT",
          storageUri: "gs://my-bucket/root/subpath",
        },
        expectError: false,
      },
      {
        testName: "invalid connection format",
        icebergConfigBlock: `
        iceberg: {
            connection: "invalid",
            storageUri: "gs://my-bucket/root/subpath",
        }`,
        expected: {},
        expectError: "The connection must be in the format `{project}.{location}.{connection_id}` or `projects/{project}/locations/{location}/connections/{connection_id}`, or be set to `DEFAULT`.",
      },
      {
        testName: "invalid storageUri format",
        icebergConfigBlock: `
        iceberg: {
            storageUri: "invalid"
        }`,
        expected: {},
        expectError: "The storage URI must be in the format `gs://{bucket_name}/{path_to_data}`.",
      },
    ];

    testCases.forEach(testCase => {
      const uniqueName = `iceberg_${testCase.testName.replace(/ /g, "_")}`;
      const tableConfig = `{
          type: "table",
          name: "${uniqueName}",
          schema: "dataset",
          database: "project",
          ${testCase.icebergConfigBlock}
      }`;

      const paramsToTest = [
        {
          filename: `${uniqueName}.sqlx`,
          fileContents: `config ${tableConfig}\nSELECT 1`,
        },
        {
          filename: `${uniqueName}.js`,
          fileContents: `publish("${uniqueName}", ${tableConfig}).query(ctx => "SELECT 1")`,
        },
      ];

      paramsToTest.forEach(params => {
        test(`${testCase.testName} in ${params.filename}`, () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          setupFiles(projectDir, params.filename, params.fileContents);

          const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

          if (testCase.expectError) {
            expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).greaterThan(0);
            const error = result.compile.compiledGraph.graphErrors.compilationErrors[0];
            expect(error.message).contains(testCase.expectError);
            expect(error.fileName).equals(`definitions/${params.filename}`);
          } else {
            expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
            const compiledTable = result.compile.compiledGraph.tables[0];
            expect(compiledTable.target.name).equals(uniqueName);
            expect(compiledTable.bigquery.tableFormat).equals(dataform.TableFormat.ICEBERG);
            expect(compiledTable.bigquery.fileFormat).equals(testCase.expected.fileFormat);
            expect(compiledTable.bigquery.connection).equals(testCase.expected.connection);
            expect(compiledTable.bigquery.storageUri).equals(testCase.expected.storageUri);
          }
        });
      });
    });
  });
});
