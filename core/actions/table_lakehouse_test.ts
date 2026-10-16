import { expect } from "chai";

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

suite("Lakehouse table options", ({ afterEach }) => {
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
      testName: "with explicit catalog and namespace",
      configBlock: `
        name: "table1",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        }`,
      expected: {
        target: { name: "table1", schema: "my_catalog.my_namespace", database: "project" },
        canonicalTarget: {
          name: "table1",
          schema: "my_catalog.my_namespace",
          database: "project",
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
        name: "table_ws_all",
        lakehouse: {}`,
      expected: {
        target: {
          name: "table_ws_all",
          schema: "ws_default_catalog.ws_default_namespace",
          database: "project",
        },
        canonicalTarget: {
          name: "table_ws_all",
          schema: "ws_default_catalog.ws_default_namespace",
          database: "project",
        },
      },
      expectError: false,
    },
    {
      testName: "uses default catalog from workflow_settings.yaml when only namespace is set",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_LAKEHOUSE_DEFAULTS,
      configBlock: `
        name: "table_ws_cat",
        lakehouse: {
          namespace: "custom_ns",
        }`,
      expected: {
        target: {
          name: "table_ws_cat",
          schema: "ws_default_catalog.custom_ns",
          database: "project",
        },
        canonicalTarget: {
          name: "table_ws_cat",
          schema: "ws_default_catalog.custom_ns",
          database: "project",
        },
      },
      expectError: false,
    },
    {
      testName: "uses default namespace from workflow_settings.yaml when only catalog is set",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_LAKEHOUSE_DEFAULTS,
      configBlock: `
        name: "table_ws_ns",
        lakehouse: {
          catalog: "custom_cat",
        }`,
      expected: {
        target: {
          name: "table_ws_ns",
          schema: "custom_cat.ws_default_namespace",
          database: "project",
        },
        canonicalTarget: {
          name: "table_ws_ns",
          schema: "custom_cat.ws_default_namespace",
          database: "project",
        },
      },
      expectError: false,
    },
    {
      testName: "action lakehouse values override workflow_settings.yaml defaults",
      wsContent: CUSTOM_WORKFLOW_SETTINGS_WITH_LAKEHOUSE_DEFAULTS,
      configBlock: `
        name: "table_override",
        lakehouse: {
          catalog: "override_cat",
          namespace: "override_ns",
        }`,
      expected: {
        target: {
          name: "table_override",
          schema: "override_cat.override_ns",
          database: "project",
        },
        canonicalTarget: {
          name: "table_override",
          schema: "override_cat.override_ns",
          database: "project",
        },
      },
      expectError: false,
    },
    {
      testName: "supports partitionBy and additionalOptions on Lakehouse table",
      configBlock: `
        name: "table_opts",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        bigquery: {
          partitionBy: "ts_col",
          additionalOptions: {
            storage_uri: "gs://my-bucket/my_namespace/table_opts",
          },
        }`,
      expected: {
        target: { name: "table_opts", schema: "my_catalog.my_namespace", database: "project" },
        canonicalTarget: {
          name: "table_opts",
          schema: "my_catalog.my_namespace",
          database: "project",
        },
        bigquery: {
          partitionBy: "ts_col",
          additionalOptions: {
            storage_uri: "gs://my-bucket/my_namespace/table_opts",
          },
        },
      },
      expectError: false,
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when lakehouse is combined with dataset",
      configBlock: `
        name: "table_err_dataset",
        dataset: "some_dataset",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        }`,
      expectError: "Cannot specify both 'lakehouse' and 'dataset'/'schema' on the same action.",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when lakehouse is combined with schema",
      configBlock: `
        name: "table_err_schema",
        schema: "some_schema",
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
        name: "table_err_iceberg",
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
      testName: "fails when catalog is missing and no default is configured",
      configBlock: `
        name: "table_no_cat",
        lakehouse: {
          namespace: "my_namespace",
        }`,
      expectError:
        "When defining a Lakehouse target, 'catalog' must be defined in workflow_settings.yaml or the lakehouse config block.",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when namespace is missing and no default is configured",
      configBlock: `
        name: "table_no_ns",
        lakehouse: {
          catalog: "my_catalog",
        }`,
      expectError:
        "When defining a Lakehouse target, 'namespace' must be defined in workflow_settings.yaml or the lakehouse config block.",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when catalog contains a dot",
      configBlock: `
        name: "table_dot_cat",
        lakehouse: {
          catalog: "my.catalog",
          namespace: "my_namespace",
        }`,
      expectError: "Action target catalog cannot include '.'",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when namespace contains a dot",
      configBlock: `
        name: "table_dot_ns",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my.namespace",
        }`,
      expectError: "Action target namespace cannot include '.'",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when inline assertions are configured on a Lakehouse table",
      configBlock: `
        name: "table_assertions",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        assertions: {
          nonNull: ["id"],
        }`,
      expectError: "Inline assertions are not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when labels are configured on a Lakehouse table",
      configBlock: `
        name: "table_labels",
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
      testName: "fails when clusterBy is configured on a Lakehouse table",
      configBlock: `
        name: "table_cluster",
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
      testName: "fails when requirePartitionFilter is configured on a Lakehouse table",
      configBlock: `
        name: "table_rpf",
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
      testName:
        "fails when require_partition_filter in additionalOptions is configured on a Lakehouse table",
      configBlock: `
        name: "table_rpf_add",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        bigquery: {
          additionalOptions: {
            require_partition_filter: "true",
          },
        }`,
      expectError: "requirePartitionFilter is not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName: "fails when partitionExpirationDays is configured on a Lakehouse table",
      configBlock: `
        name: "table_ped",
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
        "fails when partition_expiration_days in additionalOptions is configured on a Lakehouse table",
      configBlock: `
        name: "table_ped_add",
        lakehouse: {
          catalog: "my_catalog",
          namespace: "my_namespace",
        },
        bigquery: {
          additionalOptions: {
            partition_expiration_days: "7",
          },
        }`,
      expectError: "partitionExpirationDays is not supported for Lakehouse (4-part) targets",
      wsContent: VALID_WORKFLOW_SETTINGS_YAML,
    },
    {
      testName:
        "fails when column policy tags (bigqueryPolicyTags) are configured on a Lakehouse table",
      configBlock: `
        name: "table_policy_tags",
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
  ];

  testCases.forEach((testCase, index) => {
    const fileName = `lakehouse_test_${index}`;
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
          if (testCase.expected!.bigquery) {
            expect(asPlainObject(compiledTable.bigquery)).deep.equals(
              asPlainObject(testCase.expected!.bigquery),
            );
          }
        }
      });
    });
  });

  test("resolves ${self()} in query, pre_operations, and post_operations for 4-part Lakehouse table", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    setupFiles(
      projectDir,
      "lakehouse_self.sqlx",
      `
config {
  type: "table",
  project: "p",
  lakehouse: { catalog: "c", namespace: "n" },
  name: "t"
}
SELECT 1 AS id FROM \${self()}
pre_operations {
  SELECT COUNT(*) FROM \${self()}
}
post_operations {
  ASSERT ((SELECT COUNT(1) FROM \${self()}) = 1)
}
`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const compiledTable = result.compile.compiledGraph.tables[0];
    expect(asPlainObject(compiledTable.target)).deep.equals({
      database: "p",
      schema: "c.n",
      name: "t",
    });
    expect(compiledTable.query.trim()).equals("SELECT 1 AS id FROM `p.c.n.t`");
    expect(compiledTable.preOps[0].trim()).equals("SELECT COUNT(*) FROM `p.c.n.t`");
    expect(compiledTable.postOps[0].trim()).equals("ASSERT ((SELECT COUNT(1) FROM `p.c.n.t`) = 1)");
  });

  test("actions without a lakehouse block are not affected by defaultLakehouseConfig", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, CUSTOM_WORKFLOW_SETTINGS_WITH_LAKEHOUSE_DEFAULTS);
    writeDefinitionFile(
      projectDir,
      "regular_table.sqlx",
      `config { type: "table", name: "reg_tbl" }\nSELECT 1`,
    );
    writeDefinitionFile(
      projectDir,
      "lakehouse_table.sqlx",
      `config { type: "table", name: "lh_tbl", lakehouse: {} }\nSELECT * FROM \${ref("reg_tbl")}`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const regTable = result.compile.compiledGraph.tables.find((t) => t.target.name === "reg_tbl");
    const lhTable = result.compile.compiledGraph.tables.find((t) => t.target.name === "lh_tbl");
    expect(asPlainObject(regTable.target)).deep.equals({
      database: "defaultProject",
      schema: "defaultDataset",
      name: "reg_tbl",
    });
    expect(asPlainObject(lhTable.target)).deep.equals({
      database: "defaultProject",
      schema: "ws_default_catalog.ws_default_namespace",
      name: "lh_tbl",
    });
  });

  test("datasetSuffix is not applied to Lakehouse targets while projectSuffix and namePrefix still apply", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: "defaultProject"
defaultDataset: "defaultDataset"
defaultLocation: "us-central1"
projectSuffix: "psuf"
datasetSuffix: "dsuf"
namePrefix: "npref"
`,
    );
    writeDefinitionFile(
      projectDir,
      "lh_table.sqlx",
      `
config {
  type: "table",
  lakehouse: { catalog: "cat", namespace: "ns" },
  name: "tbl"
}
SELECT "\${self()}" AS self_ref
`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const compiledTable = result.compile.compiledGraph.tables[0];
    expect(asPlainObject(compiledTable.target)).deep.equals({
      database: "defaultProject_psuf",
      schema: "cat.ns",
      name: "npref_tbl",
    });
    expect(asPlainObject(compiledTable.canonicalTarget)).deep.equals({
      database: "defaultProject",
      schema: "cat.ns",
      name: "tbl",
    });
    expect(compiledTable.query.trim()).equals(
      'SELECT "`defaultProject_psuf.cat.ns.npref_tbl`" AS self_ref',
    );
  });

  test("partial Lakehouse ref applies defaultLakehouseConfig and does not fall back to 3-part table", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: defaultProject
defaultDataset: defaultDataset
defaultLocation: US
defaultLakehouseConfig:
  catalog: default_cat
  namespace: default_ns
`,
    );
    writeDefinitionFile(
      projectDir,
      "bq_tbl.sqlx",
      `
config {
  type: "table",
  name: "shared_name"
}
SELECT 1
`,
    );
    writeDefinitionFile(
      projectDir,
      "lh_tbl.sqlx",
      `
config {
  type: "table",
  name: "shared_name",
  lakehouse: {
    catalog: "custom_cat"
  }
}
SELECT 2
`,
    );
    writeDefinitionFile(
      projectDir,
      "consumer.sqlx",
      `
config {
  type: "table"
}
SELECT * FROM \${ref({ catalog: "custom_cat", name: "shared_name" })}
`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const consumer = result.compile.compiledGraph.tables.find((t) => t.target.name === "consumer");
    expect(consumer.query.trim()).equals(
      "SELECT * FROM `defaultProject.custom_cat.default_ns.shared_name`",
    );
    expect(asPlainObject(consumer.dependencyTargets)).deep.equals([
      {
        database: "defaultProject",
        schema: "custom_cat.default_ns",
        name: "shared_name",
      },
    ]);
  });

  test("partial Lakehouse ref without defaultLakehouseConfig fails instead of matching 3-part table by name", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "bq_tbl.sqlx",
      `
config {
  type: "table",
  name: "shared_name"
}
SELECT 1
`,
    );
    writeDefinitionFile(
      projectDir,
      "consumer.sqlx",
      `
config {
  type: "table"
}
SELECT * FROM \${ref({ catalog: "custom_cat", name: "shared_name" })}
`,
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
    const errors = result.compile.compiledGraph.graphErrors.compilationErrors.map((e) => e.message);
    expect(errors).to.include(
      "Lakehouse target reference must specify both 'catalog' and 'namespace' (or set defaultLakehouseConfig in workflow_settings.yaml).",
    );
  });
});
