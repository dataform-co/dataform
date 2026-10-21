import { expect } from "chai";

import { dataform } from "df/protos/ts";
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

suite("Lakehouse incremental table options", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  const setupFiles = (
    projectDir: string,
    filename: string,
    fileContents: string,
    wsContent: string = VALID_WORKFLOW_SETTINGS_YAML,
  ) => {
    writeWorkflowSettingsFile(projectDir, wsContent);
    writeDefinitionFile(projectDir, filename, fileContents);
  };

  const CUSTOM_WORKFLOW_SETTINGS_WITH_LAKEHOUSE_DEFAULTS = `
defaultProject: "defaultProject"
defaultDataset: "defaultDataset"
defaultLocation: "us-central1"
defaultLakehouseConfig:
  catalog: "ws_default_catalog"
  namespace: "ws_default_namespace"
`;

  const testCases = [
    {
      testName: "with explicit catalog, namespace, and uniqueKey",
      configBlock: `
        type: "incremental",
        name: "inc_table1",
        uniqueKey: ["id"],
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        }`,
      expected: {
        target: {
          name: "inc_table1",
          schema: "my_catalog.my_namespace",
          database: "defaultProject",
        },
        canonicalTarget: {
          name: "inc_table1",
          schema: "my_catalog.my_namespace",
          database: "defaultProject",
        },
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName:
        "uses default catalog and namespace from workflow_settings.yaml when lakehouse is empty",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_LAKEHOUSE_DEFAULTS,
      configBlock: `
        type: "incremental",
        name: "inc_table_ws_all",
        lakehouse: {}`,
      expected: {
        target: {
          name: "inc_table_ws_all",
          schema: "ws_default_catalog.ws_default_namespace",
          database: "defaultProject",
        },
        canonicalTarget: {
          name: "inc_table_ws_all",
          schema: "ws_default_catalog.ws_default_namespace",
          database: "defaultProject",
        },
      },
      expectError: false,
    },
    {
      testName: "action lakehouse values override workflow_settings.yaml defaults",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_LAKEHOUSE_DEFAULTS,
      configBlock: `
        type: "incremental",
        name: "inc_table_override",
        lakehouse: {
          catalog: "override_cat",
          namespace: "override_ns",
        }`,
      expected: {
        target: {
          name: "inc_table_override",
          schema: "override_cat.override_ns",
          database: "defaultProject",
        },
        canonicalTarget: {
          name: "inc_table_override",
          schema: "override_cat.override_ns",
          database: "defaultProject",
        },
      },
      expectError: false,
    },
    {
      testName: "fails when lakehouse is combined with dataset",
      configBlock: `
        type: "incremental",
        name: "inc_err_dataset",
        dataset: "some_dataset",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        }`,
      expectError: "Cannot specify both 'lakehouse' and 'dataset'/'schema' on the same action.",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when lakehouse is combined with iceberg",
      configBlock: `
        type: "incremental",
        name: "inc_err_iceberg",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        bigquery: {
          iceberg: {
            bucketName: "my-bucket",
          },
        }`,
      expectError: "Cannot specify both 'lakehouse' and 'iceberg' on the same action.",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when inline assertions are configured on a Lakehouse incremental table",
      configBlock: `
        type: "incremental",
        name: "inc_err_assertions",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        assertions: {
          uniqueKey: ["id"],
        }`,
      expectError: "Inline assertions are not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when labels are configured on a Lakehouse incremental table",
      configBlock: `
        type: "incremental",
        name: "inc_err_labels",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        bigquery: {
          labels: { env: "prod" },
        }`,
      expectError: "BigQuery labels are not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when clusterBy is configured on a Lakehouse incremental table",
      configBlock: `
        type: "incremental",
        name: "inc_err_cluster",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        bigquery: {
          clusterBy: ["id"],
        }`,
      expectError: "clusterBy is not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when requirePartitionFilter is configured on a Lakehouse incremental table",
      configBlock: `
        type: "incremental",
        name: "inc_err_rpf",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        bigquery: {
          partitionBy: "ts",
          requirePartitionFilter: true,
        }`,
      expectError: "requirePartitionFilter is not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when partitionExpirationDays is configured on a Lakehouse incremental table",
      configBlock: `
        type: "incremental",
        name: "inc_err_ped",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        bigquery: {
          partitionBy: "ts",
          partitionExpirationDays: 7,
        }`,
      expectError: "partitionExpirationDays is not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName:
        "fails when column policy tags (bigqueryPolicyTags) are configured on a Lakehouse incremental table",
      configBlock: `
        type: "incremental",
        name: "inc_err_policy_tags",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        columns: {
          id: {
            description: "Primary key",
            bigqueryPolicyTags: ["projects/p/locations/us/taxonomies/1/policyTags/2"],
          },
        }`,
      expectError:
        "Column policy tags (bigqueryPolicyTags) are not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when defaultDataset is not set in workflow_settings.yaml",
      configBlock: `
        type: "incremental",
        name: "inc_err_no_default_dataset",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        }`,
      expectError:
        "When defining an incremental Lakehouse table, 'defaultDataset' must be defined in workflow_settings.yaml.",
      wsContent: `
defaultProject: "defaultProject"
defaultLocation: "us-central1"
`,
    },
  ];

  testCases.forEach((testCase, index) => {
    const fileName = `lakehouse_inc_test_${index}`;
    const tableConfig = `{
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

    paramsToTest.forEach((params) => {
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
          expect(asPlainObject(compiledTable.target)).deep.equals(testCase.expected!.target);
          expect(asPlainObject(compiledTable.canonicalTarget)).deep.equals(
            testCase.expected!.canonicalTarget,
          );
        }
      });
    });
  });

  test("resolves ${self()} in query and incrementalQuery for 4-part Lakehouse incremental table", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    setupFiles(
      projectDir,
      "lakehouse_inc_self.sqlx",
      `
config {
  type: "incremental",
  project: "p",
  lakehouse: { catalog: "c", namespace: "n" },
  name: "inc_t",
  uniqueKey: ["id"]
}
SELECT 1 AS id \${when(incremental(), \`FROM \${self()}\`)}
`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const compiledTable = result.compile.compiledGraph.tables[0];
    expect(asPlainObject(compiledTable.target)).deep.equals({
      database: "p",
      schema: "c.n",
      name: "inc_t",
    });
    expect(compiledTable.query.trim()).equals("SELECT 1 AS id");
    expect(compiledTable.incrementalQuery.trim()).equals("SELECT 1 AS id FROM `p.c.n.inc_t`");
    expect(compiledTable.uniqueKey).deep.equals(["id"]);
  });

  test("succeeds when defaultDataset is omitted in workflow_settings.yaml but provided via compilation override", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    setupFiles(
      projectDir,
      "lakehouse_inc_override.sqlx",
      `
config {
  type: "incremental",
  lakehouse: { catalog: "c", namespace: "n" },
  name: "inc_override_t",
}
SELECT 1 AS id
`,
      `
defaultProject: "defaultProject"
defaultLocation: "us-central1"
`,
    );

    const result = runMainInVm(
      coreExecutionRequestFromPath(
        projectDir,
        dataform.ProjectConfig.create({ defaultSchema: "overriddenDataset" }),
      ),
    );

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const compiledTable = result.compile.compiledGraph.tables[0];
    expect(asPlainObject(compiledTable.target)).deep.equals({
      database: "defaultProject",
      schema: "c.n",
      name: "inc_override_t",
    });
  });
});
