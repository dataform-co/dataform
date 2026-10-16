import { expect } from "chai";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import {
  CREDENTIALS_PATH,
  INTEGRATION_TEST_LOCATION,
  INTEGRATION_TEST_PROJECT,
} from "df/cli/index_test_base";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { test } from "df/testing";
import { integrationSuite } from "df/testing/credentials";
import { dropDatasets, dropStaleDatasets, makeUniqueSuffix } from "df/testing/bigquery";
import { compile, getTableRows, keyBy } from "df/tests/integration/utils";

// Each test compiles bigquery_project with its own schema suffix, which dataform appends to every
// dataset of the project. The unique suffix is appended to the names below.
const PROJECT_E2E_SCHEMA_SUFFIX = "project_e2e";
const PROJECT_E2E_DATASET = `df_integration_test_${PROJECT_E2E_SCHEMA_SUFFIX}`;
const PROJECT_E2E_ASSERTIONS_DATASET = `df_integration_test_assertions_${PROJECT_E2E_SCHEMA_SUFFIX}`;
const PROJECT_E2E_TEST_DATASET = `test_${PROJECT_E2E_SCHEMA_SUFFIX}`;

const INCREMENTAL_TABLES_SCHEMA_SUFFIX = "incremental_tables";
const INCREMENTAL_TABLES_DATASET = `df_integration_test_${INCREMENTAL_TABLES_SCHEMA_SUFFIX}`;
const INCREMENTAL_TABLES_ASSERTIONS_DATASET = `df_integration_test_assertions_${INCREMENTAL_TABLES_SCHEMA_SUFFIX}`;

const DATASET_METADATA_SCHEMA_SUFFIX = "dataset_metadata";
const DATASET_METADATA_DATASET = `df_integration_test_${DATASET_METADATA_SCHEMA_SUFFIX}`;
const DATASET_METADATA_ASSERTIONS_DATASET = `df_integration_test_assertions_${DATASET_METADATA_SCHEMA_SUFFIX}`;

const DATASETS = [
  PROJECT_E2E_DATASET,
  PROJECT_E2E_ASSERTIONS_DATASET,
  PROJECT_E2E_TEST_DATASET,
  INCREMENTAL_TABLES_DATASET,
  INCREMENTAL_TABLES_ASSERTIONS_DATASET,
  DATASET_METADATA_DATASET,
  DATASET_METADATA_ASSERTIONS_DATASET,
];

integrationSuite("@dataform/integration/run", { parallel: true }, ({ before, after }) => {
  const credentials = dfapi.credentials.read(CREDENTIALS_PATH);
  const dbadapter = new BigQueryDbAdapter(credentials);
  const uniqueSuffix = makeUniqueSuffix();

  before("drop stale datasets", async () => {
    await dropStaleDatasets(dbadapter, {
      project: INTEGRATION_TEST_PROJECT,
      location: INTEGRATION_TEST_LOCATION,
      prefixes: DATASETS,
    });
  });

  after("drop datasets", async () => {
    await dropDatasets(
      dbadapter,
      INTEGRATION_TEST_PROJECT,
      DATASETS.map((dataset) => `${dataset}_${uniqueSuffix}`),
    );
  });

  test("project e2e", { timeout: 60000 }, async () => {
    // The datasets are unique to this run, so they don't exist yet and schema creation is tested.
    const compiledGraph = await compile(
      "tests/integration/bigquery_project",
      `${PROJECT_E2E_SCHEMA_SUFFIX}_${uniqueSuffix}`,
    );
    const dataset = `${PROJECT_E2E_DATASET}_${uniqueSuffix}`;
    const assertionsDataset = `${PROJECT_E2E_ASSERTIONS_DATASET}_${uniqueSuffix}`;

    // Run the project.
    const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    const executedGraph = await dfapi.run(dbadapter, executionGraph).result();

    const actionMap = keyBy(executedGraph.actions, (v) => targetAsReadableString(v.target));
    expect(Object.keys(actionMap).length).eql(20);

    // Check the status of action execution.
    const expectedFailedActions = [
      `${INTEGRATION_TEST_PROJECT}.${assertionsDataset}.example_assertion_fail`,
      `${INTEGRATION_TEST_PROJECT}.${dataset}.example_operation_partial_fail`,
    ];
    for (const actionName of Object.keys(actionMap)) {
      const expectedResult = expectedFailedActions.includes(actionName)
        ? dataform.ActionResult.ExecutionStatus.FAILED
        : dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
      expect(actionMap[actionName].status).equals(
        expectedResult,
        JSON.stringify(actionMap[actionName], null, 4),
      );
    }

    expect(
      actionMap[`${INTEGRATION_TEST_PROJECT}.${assertionsDataset}.example_assertion_fail`].tasks[1]
        .errorMessage,
    ).to.eql("bigquery error: Assertion failed: query returned 1 row(s).");

    expect(
      actionMap[`${INTEGRATION_TEST_PROJECT}.${dataset}.example_operation_partial_fail`].tasks[0]
        .errorMessage,
    ).to.eql("bigquery error: Query error: Unrecognized name: invalid_column at [3:8]");
  });

  test("incremental tables", { timeout: 60000 }, async () => {
    const compiledGraph = await compile(
      "tests/integration/bigquery_project",
      `${INCREMENTAL_TABLES_SCHEMA_SUFFIX}_${uniqueSuffix}`,
    );
    const dataset = `${INCREMENTAL_TABLES_DATASET}_${uniqueSuffix}`;

    // Run two iterations of the project.
    const adapter = new ExecutionSql(
      compiledGraph.projectConfig,
      compiledGraph.dataformCoreVersion,
    );
    for (const runIteration of [
      {
        runConfig: {
          actions: ["example_incremental", "example_incremental_merge"],
          includeDependencies: true,
        },
        expectedIncrementalRows: 3,
        expectedIncrementalMergeRows: 2,
      },
      {
        runConfig: {
          actions: ["example_incremental", "example_incremental_merge"],
        },
        expectedIncrementalRows: 5,
        expectedIncrementalMergeRows: 2,
      },
    ]) {
      const executionGraph = await dfapi.build(compiledGraph, runIteration.runConfig, dbadapter);
      const runResult = await dfapi.run(dbadapter, executionGraph).result();
      expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
        dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL],
      );
      const [incrementalRows, incrementalMergeRows] = await Promise.all([
        getTableRows(
          {
            database: INTEGRATION_TEST_PROJECT,
            schema: dataset,
            name: "example_incremental",
          },
          adapter,
          dbadapter,
        ),
        getTableRows(
          {
            database: INTEGRATION_TEST_PROJECT,
            schema: dataset,
            name: "example_incremental_merge",
          },
          adapter,
          dbadapter,
        ),
      ]);
      expect(incrementalRows.length).equals(runIteration.expectedIncrementalRows);
      expect(incrementalMergeRows.length).equals(runIteration.expectedIncrementalMergeRows);
    }
  });

  test("dataset metadata set correctly", { timeout: 60000 }, async () => {
    const compiledGraph = await compile(
      "tests/integration/bigquery_project",
      `${DATASET_METADATA_SCHEMA_SUFFIX}_${uniqueSuffix}`,
    );
    const dataset = `${DATASET_METADATA_DATASET}_${uniqueSuffix}`;

    // Run the project.
    const executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["example_incremental", "example_view"],
        includeDependencies: true,
      },
      dbadapter,
    );
    const runResult = await dfapi.run(dbadapter, executionGraph).result();
    expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
      dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL],
    );

    // Check expected metadata.
    for (const expectedMetadata of [
      {
        target: {
          database: INTEGRATION_TEST_PROJECT,
          schema: dataset,
          name: "example_incremental",
        },
        expectedDescription: "An incremental table",
        expectedFields: [
          dataform.Field.create({
            description: "the timestamp",
            name: "user_timestamp",
            primitive: dataform.Field.Primitive.INTEGER,
          }),
          dataform.Field.create({
            description: "the id",
            name: "user_id",
            primitive: dataform.Field.Primitive.INTEGER,
          }),
          dataform.Field.create({
            name: "nested_data",
            description: "some nested data with duplicate fields",
            struct: dataform.Fields.create({
              fields: [
                dataform.Field.create({
                  description: "nested timestamp",
                  name: "user_timestamp",
                  primitive: dataform.Field.Primitive.INTEGER,
                }),
                dataform.Field.create({
                  description: "nested id",
                  name: "user_id",
                  primitive: dataform.Field.Primitive.INTEGER,
                }),
              ],
            }),
          }),
        ],
        expectedLabels: {},
      },
      {
        target: {
          database: INTEGRATION_TEST_PROJECT,
          schema: dataset,
          name: "example_view",
        },
        expectedDescription: "An example view",
        expectedFields: [
          dataform.Field.create({
            description: "val doc",
            name: "val",
            primitive: dataform.Field.Primitive.INTEGER,
          }),
        ],
        expectedLabels: {
          label1: "val1",
          label2: "val2",
        },
      },
    ]) {
      const metadata = await dbadapter.table(expectedMetadata.target);
      expect(metadata.description).to.equal(expectedMetadata.expectedDescription);
      expect(metadata.fields).to.deep.equal(expectedMetadata.expectedFields);
      expect(metadata.labels).to.deep.equal(expectedMetadata.expectedLabels);
    }
  });
});
