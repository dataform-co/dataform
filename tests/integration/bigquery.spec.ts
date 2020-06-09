import { expect } from "chai";
import Long from "long";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import { BigQueryDbAdapter } from "df/api/dbadapters/bigquery";
import { hashExecutionAction } from "df/api/utils/run_cache";
import * as adapters from "df/core/adapters";
import { BigQueryAdapter } from "df/core/adapters/bigquery";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

const EXPECTED_INCREMENTAL_EXAMPLE_SCHEMA = {
  fields: [
    {
      description: "the timestamp",
      name: "user_timestamp",
      type: "INTEGER"
    },
    {
      description: "the id",
      name: "user_id",
      type: "INTEGER"
    },
    {
      name: "nested_data",
      description: "some nested data with duplicate fields",
      type: "RECORD",
      fields: [
        {
          description: "nested timestamp",
          name: "user_timestamp",
          type: "INTEGER"
        },
        {
          description: "nested id",
          name: "user_id",
          type: "INTEGER"
        }
      ]
    }
  ]
};
const EXPECTED_EXAMPLE_VIEW_SCHEMA = {
  fields: [
    {
      description: "val doc",
      name: "val",
      type: "INTEGER"
    }
  ]
};

suite("@dataform/integration/bigquery", { parallel: true }, ({ before, after }) => {
  const credentials = dfapi.credentials.read("bigquery", "test_credentials/bigquery.json");
  let dbadapter: BigQueryDbAdapter;

  before("create adapter", async () => {
    dbadapter = (await dbadapters.create(credentials, "bigquery")) as BigQueryDbAdapter;
  });

  after("close adapter", () => dbadapter.close());

  suite("run", { parallel: true }, () => {
    test("project e2e", { timeout: 60000 }, async () => {
      const compiledGraph = await compile("project_e2e");

      // Drop all the tables before we do anything.
      await cleanWarehouse(compiledGraph, dbadapter);

      // Drop schemas to make sure schema creation works.
      await dbadapter.dropSchema("dataform-integration-tests", "df_integration_test_project_e2e");

      // Run the project.
      const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
      const executedGraph = await dfapi.run(executionGraph, dbadapter).result();

      const actionMap = keyBy(executedGraph.actions, v => v.name);
      expect(Object.keys(actionMap).length).eql(14);

      // Check the status of action execution.
      const expectedFailedActions = [
        "dataform-integration-tests.df_integration_test_assertions_project_e2e.example_assertion_uniqueness_fail",
        "dataform-integration-tests.df_integration_test_assertions_project_e2e.example_assertion_fail"
      ];
      for (const actionName of Object.keys(actionMap)) {
        const expectedResult = expectedFailedActions.includes(actionName)
          ? dataform.ActionResult.ExecutionStatus.FAILED
          : dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
        expect(actionMap[actionName].status).equals(expectedResult);
      }

      expect(
        actionMap[
          "dataform-integration-tests.df_integration_test_assertions_project_e2e.example_assertion_uniqueness_fail"
        ].tasks[1].errorMessage
      ).to.eql("bigquery error: Assertion failed: query returned 1 row(s).");
    });

    test("run caching", { timeout: 60000 }, async () => {
      const compiledGraph = await compile("run_caching", { useRunCache: true });

      // Drop all the tables before we do anything.
      await cleanWarehouse(compiledGraph, dbadapter);

      // Drop the meta schema
      await dbadapter.dropSchema("dataform-integration-tests", "dataform_meta");

      // Run the project.
      let executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
      let executedGraph = await dfapi.run(executionGraph, dbadapter).result();

      // run cache assertions
      executionGraph = await dfapi.build(
        compiledGraph,
        {
          actions: [
            "example_incremental",
            "example_incremental_merge",
            "example_table",
            "example_view",
            "example_assertion_fail",
            "example_operation",
            "depends_on_example_view"
          ]
        },
        dbadapter
      );

      executedGraph = await dfapi.run(executionGraph, dbadapter).result();
      let actionMap = keyBy(executedGraph.actions, v => v.name);

      expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.FAILED);

      let expectedActionStatus: { [index: string]: dataform.ActionResult.ExecutionStatus } = {
        "dataform-integration-tests.df_integration_test_run_caching.example_incremental":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        "dataform-integration-tests.df_integration_test_run_caching.example_incremental_merge":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        "dataform-integration-tests.df_integration_test_run_caching.example_table":
          dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED,
        "dataform-integration-tests.df_integration_test_run_caching.example_view":
          dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED,
        "dataform-integration-tests.df_integration_test_assertions_run_caching.example_assertion_fail":
          dataform.ActionResult.ExecutionStatus.FAILED,
        "dataform-integration-tests.df_integration_test_run_caching.example_operation":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        "dataform-integration-tests.df_integration_test_run_caching.depends_on_example_view":
          dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED
      };

      for (const actionName of Object.keys(actionMap)) {
        expect(
          dataform.ActionResult.ExecutionStatus[actionMap[actionName].status],
          `ActionResult ExecutionStatus for action "${actionName}"`
        ).equals(dataform.ActionResult.ExecutionStatus[expectedActionStatus[actionName]]);
      }

      const persistedMetaData = await dbadapter.persistedStateMetadata();
      expect(persistedMetaData.length).to.be.eql(7);

      const exampleView = persistedMetaData.find(table => table.target.name === "example_view");
      const exampleViewExecutionAction = executionGraph.actions.find(
        action =>
          action.name === "dataform-integration-tests.df_integration_test_run_caching.example_view"
      );
      expect(exampleView.definitionHash).to.eql(hashExecutionAction(exampleViewExecutionAction));

      const exampleAssertionFail = persistedMetaData.find(
        table => table.target.name === "example_assertion_fail"
      );
      expect(exampleAssertionFail).to.be.eql(undefined);

      compiledGraph.tables = compiledGraph.tables.map(table => {
        if (
          table.name === "dataform-integration-tests.df_integration_test_run_caching.example_view"
        ) {
          table.query = "select 1 as val";
        }
        return table;
      });

      executionGraph = await dfapi.build(
        compiledGraph,
        {
          actions: ["example_view", "depends_on_example_view"]
        },
        dbadapter
      );

      executedGraph = await dfapi.run(executionGraph, dbadapter).result();
      expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
      actionMap = keyBy(executedGraph.actions, v => v.name);
      expectedActionStatus = {
        "dataform-integration-tests.df_integration_test_run_caching.example_view":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        "dataform-integration-tests.df_integration_test_run_caching.depends_on_example_view":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL
      };

      for (const actionName of Object.keys(actionMap)) {
        expect(actionMap[actionName].status).equals(expectedActionStatus[actionName]);
      }
    });

    test("incremental tables", { timeout: 60000 }, async () => {
      const compiledGraph = await compile("incremental_tables");

      // Drop all the tables before we do anything.
      await cleanWarehouse(compiledGraph, dbadapter);

      // Run the project.
      let executionGraph = await dfapi.build(
        compiledGraph,
        {
          actions: ["example_incremental", "example_incremental_merge"],
          includeDependencies: true
        },
        dbadapter
      );
      let runResult = await dfapi.run(executionGraph, dbadapter).result();
      expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
        dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
      );

      // Check the data in the incremental tables.
      const adapter = adapters.create(
        compiledGraph.projectConfig,
        compiledGraph.dataformCoreVersion
      );
      const [incrementalRows, incrementalMergeRows] = await Promise.all([
        getTableRows(
          {
            database: "dataform-integration-tests",
            schema: "df_integration_test_incremental_tables",
            name: "example_incremental"
          },
          adapter,
          dbadapter
        ),
        getTableRows(
          {
            database: "dataform-integration-tests",
            schema: "df_integration_test_incremental_tables",
            name: "example_incremental_merge"
          },
          adapter,
          dbadapter
        )
      ]);
      expect(incrementalRows.length).equals(3);
      expect(incrementalMergeRows.length).equals(2);

      // Re-run incremental actions.
      executionGraph = await dfapi.build(
        compiledGraph,
        {
          actions: ["example_incremental", "example_incremental_merge"]
        },
        dbadapter
      );
      runResult = await dfapi.run(executionGraph, dbadapter).result();
      expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
        dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
      );

      // Check there are the expected number of extra rows in the incremental tables.
      const [incrementalRowsAfterSecondRun, incrementalMergeRowsAfterSecondRun] = await Promise.all(
        [
          getTableRows(
            {
              database: "dataform-integration-tests",
              schema: "df_integration_test_incremental_tables",
              name: "example_incremental"
            },
            adapter,
            dbadapter
          ),
          getTableRows(
            {
              database: "dataform-integration-tests",
              schema: "df_integration_test_incremental_tables",
              name: "example_incremental_merge"
            },
            adapter,
            dbadapter
          )
        ]
      );
      expect(incrementalRowsAfterSecondRun.length).equals(5);
      expect(incrementalMergeRowsAfterSecondRun.length).equals(2);
    });

    test("dataset metadata set correctly", { timeout: 60000 }, async () => {
      const compiledGraph = await compile("dataset_metadata");

      // Drop all the tables before we do anything.
      await cleanWarehouse(compiledGraph, dbadapter);

      // Run the project.
      const executionGraph = await dfapi.build(
        compiledGraph,
        {
          actions: ["example_incremental", "example_view"],
          includeDependencies: true
        },
        dbadapter
      );
      const runResult = await dfapi.run(executionGraph, dbadapter).result();
      expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
        dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
      );

      // Check expected metadata.
      const incrementalMetadata = await dbadapter.getMetadata({
        database: "dataform-integration-tests",
        schema: "df_integration_test_dataset_metadata",
        name: "example_incremental"
      });
      expect(incrementalMetadata.description).to.equal("An incremental table");
      expect(incrementalMetadata.schema).to.deep.equal(EXPECTED_INCREMENTAL_EXAMPLE_SCHEMA);

      const viewMetadata = await dbadapter.getMetadata({
        database: "dataform-integration-tests",
        schema: "df_integration_test_dataset_metadata",
        name: "example_view"
      });
      expect(viewMetadata.description).to.equal("An example view");
      expect(viewMetadata.schema).to.deep.equal(EXPECTED_EXAMPLE_VIEW_SCHEMA);
    });
  });

  test("run unit tests", async () => {
    const compiledGraph = await compile("unit_tests");

    // Run the tests.
    const testResults = await dfapi.test(dbadapter, compiledGraph.tests);
    expect(testResults).to.eql([
      { name: "successful", successful: true },
      {
        name: "expected more rows than got",
        successful: false,
        messages: ["Expected 3 rows, but saw 2 rows."]
      },
      {
        name: "expected fewer columns than got",
        successful: false,
        messages: ['Expected columns "col1,col2,col3", but saw "col1,col2,col3,col4".']
      },
      {
        name: "wrong columns",
        successful: false,
        messages: ['Expected columns "col1,col2,col3,col4", but saw "col1,col2,col3,col5".']
      },
      {
        name: "wrong row contents",
        successful: false,
        messages: [
          'For row 0 and column "col2": expected "1" (number), but saw "5" (number).',
          'For row 1 and column "col3": expected "6.5" (number), but saw "12" (number).',
          'For row 2 and column "col1": expected "sup?" (string), but saw "WRONG" (string).'
        ]
      }
    ]);
  });

  suite("publish tasks", { parallel: true }, async () => {
    test("incremental pre and post ops, core version <= 1.4.8", async () => {
      // 1.4.8 used `preOps` and `postOps` instead of `incrementalPreOps` and `incrementalPostOps`.
      const table: dataform.ITable = {
        type: "incremental",
        query: "query",
        preOps: ["preop task1", "preop task2"],
        incrementalQuery: "",
        postOps: ["postop task1", "postop task2"],
        target: { schema: "", name: "", database: "" }
      };

      const bqadapter = new BigQueryAdapter({ warehouse: "bigquery" }, "1.4.8");

      const refresh = bqadapter.publishTasks(table, { fullRefresh: true }, { fields: [] }).build();

      expect(refresh[0].statement).to.equal(table.preOps[0]);
      expect(refresh[1].statement).to.equal(table.preOps[1]);
      expect(refresh[refresh.length - 2].statement).to.equal(table.postOps[0]);
      expect(refresh[refresh.length - 1].statement).to.equal(table.postOps[1]);

      const increment = bqadapter
        .publishTasks(table, { fullRefresh: false }, { fields: [] })
        .build();

      expect(increment[0].statement).to.equal(table.preOps[0]);
      expect(increment[1].statement).to.equal(table.preOps[1]);
      expect(increment[increment.length - 2].statement).to.equal(table.postOps[0]);
      expect(increment[increment.length - 1].statement).to.equal(table.postOps[1]);
    });
  });

  suite("execute", { parallel: true }, async () => {
    test("returned metadata includes jobReference and statistics", async () => {
      const query = `select 1 as test`;
      const { metadata } = await dbadapter.execute(query);
      const { bigquery: bqMetadata } = metadata;
      expect(bqMetadata).to.have.property("jobId");
      expect(bqMetadata.jobId).to.match(
        /^dataform-[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}$/
      );
      expect(bqMetadata).to.have.property("totalBytesBilled");
      expect(bqMetadata.totalBytesBilled).to.eql(Long.fromNumber(0));
      expect(bqMetadata).to.have.property("totalBytesProcessed");
      expect(bqMetadata.totalBytesProcessed).to.eql(Long.fromNumber(0));
    });

    suite("result limit works", { parallel: true }, async () => {
      const query = `
        select 1 union all
        select 2 union all
        select 3 union all
        select 4 union all
        select 5`;

      for (const interactive of [true, false]) {
        test(`with interactive=${interactive}`, async () => {
          const { rows } = await dbadapter.execute(query, { interactive, maxResults: 2 });
          expect(rows).to.eql([
            {
              f0_: 1
            },
            {
              f0_: 2
            }
          ]);
        });
      }
    });
  });
});

async function compile(
  schemaSuffixOverride: string,
  projectConfigOverrides?: dataform.IProjectConfig
) {
  const compiledGraph = await dfapi.compile({
    projectDir: "tests/integration/bigquery_project",
    schemaSuffixOverride
  });

  expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);
  expect(compiledGraph.graphErrors.validationErrors).to.eql([]);

  compiledGraph.projectConfig = {
    ...compiledGraph.projectConfig,
    ...projectConfigOverrides
  };
  return compiledGraph;
}

async function cleanWarehouse(
  compiledGraph: dataform.CompiledGraph,
  dbadapter: dbadapters.IDbAdapter
) {
  await dropAllTables(
    (await dfapi.build(compiledGraph, {}, dbadapter)).warehouseState.tables,
    adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion),
    dbadapter
  );
}
