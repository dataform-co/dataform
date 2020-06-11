import { expect } from "chai";
import Long from "long";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import { BigQueryDbAdapter } from "df/api/dbadapters/bigquery";
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
      expect(Object.keys(actionMap).length).eql(16);

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

      // Re-run (some of) the project. Each included action should cache.
      executionGraph = await dfapi.build(
        compiledGraph,
        {
          actions: [
            "example_table",
            "example_view",
            "depends_on_example_view",
            "sample_data_2",
            "depends_on_sample_data_3"
          ]
        },
        dbadapter
      );
      executedGraph = await dfapi.run(executionGraph, dbadapter).result();
      for (const action of executedGraph.actions) {
        expect(
          dataform.ActionResult.ExecutionStatus[action.status],
          `ActionResult ExecutionStatus for action "${action.name}"`
        ).equals(
          dataform.ActionResult.ExecutionStatus[dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED]
        );
      }

      // Manually change some datasets (to model a data change happening outside of a DF run).
      await Promise.all([
        dbadapter.execute(
          "create or replace view `dataform-integration-tests.df_integration_test_run_caching.sample_data_2` as select 'new' as foo"
        ),
        dbadapter.execute(
          "create or replace view `dataform-integration-tests.df_integration_test_run_caching.sample_data_3` as select 'old' as bar"
        )
      ]);

      // Make a change to the 'example_view' query (to model an ExecutionAction hash change).
      compiledGraph.tables = compiledGraph.tables.map(table => {
        if (
          table.name === "dataform-integration-tests.df_integration_test_run_caching.example_view"
        ) {
          table.query = "select 1 as test";
        }
        return table;
      });

      // Re-run the project, checking caching results.
      executionGraph = await dfapi.build(
        compiledGraph,
        {
          actions: [
            "example_incremental",
            "example_table",
            "example_assertion_fail",
            "example_view",
            "depends_on_example_view",
            "sample_data_2",
            "depends_on_sample_data_3"
          ]
        },
        dbadapter
      );

      executedGraph = await dfapi.run(executionGraph, dbadapter).result();
      const actionMap = keyBy(executedGraph.actions, v => v.name);

      const expectedActionStatus: { [index: string]: dataform.ActionResult.ExecutionStatus } = {
        // Should run because it is non-hermetic.
        "dataform-integration-tests.df_integration_test_run_caching.example_incremental":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        // Should run because it failed on the last run.
        "dataform-integration-tests.df_integration_test_assertions_run_caching.example_assertion_fail":
          dataform.ActionResult.ExecutionStatus.FAILED,
        // Should run because its query definition (and thus ExecutionAction hash) has changed.
        "dataform-integration-tests.df_integration_test_run_caching.example_view":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        // Should run because the dataset has changed in the warehouse.
        "dataform-integration-tests.df_integration_test_run_caching.sample_data_2":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        // Should run because an input to dataset has changed in the warehouse.
        "dataform-integration-tests.df_integration_test_run_caching.depends_on_sample_data_3":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        // Should run because a transitive input (included in the run) did not cache.
        "dataform-integration-tests.df_integration_test_run_caching.depends_on_example_view":
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        // Should cache because it is unchanged and its singular transitive input has also not changed.
        "dataform-integration-tests.df_integration_test_run_caching.example_table":
          dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED
      };

      for (const actionName of Object.keys(actionMap)) {
        expect(
          dataform.ActionResult.ExecutionStatus[actionMap[actionName].status],
          `ActionResult ExecutionStatus for action "${actionName}"`
        ).equals(dataform.ActionResult.ExecutionStatus[expectedActionStatus[actionName]]);
      }
    });

    test("incremental tables", { timeout: 60000 }, async () => {
      const compiledGraph = await compile("incremental_tables");

      // Drop all the tables before we do anything.
      await cleanWarehouse(compiledGraph, dbadapter);

      // Run two iterations of the project.
      const adapter = adapters.create(
        compiledGraph.projectConfig,
        compiledGraph.dataformCoreVersion
      );
      for (const runIteration of [
        {
          runConfig: {
            actions: ["example_incremental", "example_incremental_merge"],
            includeDependencies: true
          },
          expectedIncrementalRows: 3,
          expectedIncrementalMergeRows: 2
        },
        {
          runConfig: {
            actions: ["example_incremental", "example_incremental_merge"]
          },
          expectedIncrementalRows: 5,
          expectedIncrementalMergeRows: 2
        }
      ]) {
        const executionGraph = await dfapi.build(compiledGraph, runIteration.runConfig, dbadapter);
        const runResult = await dfapi.run(executionGraph, dbadapter).result();
        expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
          dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
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
        expect(incrementalRows.length).equals(runIteration.expectedIncrementalRows);
        expect(incrementalMergeRows.length).equals(runIteration.expectedIncrementalMergeRows);
      }
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
      for (const expectedMetadata of [
        {
          target: {
            database: "dataform-integration-tests",
            schema: "df_integration_test_dataset_metadata",
            name: "example_incremental"
          },
          expectedDescription: "An incremental table",
          expectedSchema: EXPECTED_INCREMENTAL_EXAMPLE_SCHEMA
        },
        {
          target: {
            database: "dataform-integration-tests",
            schema: "df_integration_test_dataset_metadata",
            name: "example_view"
          },
          expectedDescription: "An example view",
          expectedSchema: EXPECTED_EXAMPLE_VIEW_SCHEMA
        }
      ]) {
        const metadata = await dbadapter.getMetadata(expectedMetadata.target);
        expect(metadata.description).to.equal(expectedMetadata.expectedDescription);
        expect(metadata.schema).to.deep.equal(expectedMetadata.expectedSchema);
      }
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

  suite("evaluate", async () => {
    test("evaluate from valid compiled graph as valid", async () => {
      // Create and run the project.
      const compiledGraph = await compile("evaluate", {
        useSingleQueryPerAction: true,
        useRunCache: false
      });
      const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
      await dfapi.run(executionGraph, dbadapter).result();

      const view = keyBy(compiledGraph.tables, t => t.name)[
        "dataform-integration-tests.df_integration_test_evaluate.example_view"
      ];
      let evaluations = await dbadapter.evaluate(dataform.Table.create(view));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const table = keyBy(compiledGraph.tables, t => t.name)[
        "dataform-integration-tests.df_integration_test_evaluate.example_table"
      ];
      evaluations = await dbadapter.evaluate(dataform.Table.create(table));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const operation = keyBy(compiledGraph.operations, t => t.name)[
        "dataform-integration-tests.df_integration_test_evaluate.example_operation"
      ];
      evaluations = await dbadapter.evaluate(dataform.Operation.create(operation));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const assertion = keyBy(compiledGraph.assertions, t => t.name)[
        "dataform-integration-tests.df_integration_test_assertions_evaluate.example_assertion_pass"
      ];
      evaluations = await dbadapter.evaluate(dataform.Assertion.create(assertion));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const incremental = keyBy(compiledGraph.tables, t => t.name)[
        "dataform-integration-tests.df_integration_test_evaluate.example_incremental"
      ];
      evaluations = await dbadapter.evaluate(dataform.Table.create(incremental));
      expect(evaluations.length).to.equal(2);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );
      expect(evaluations[1].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );
    });

    test("variable persistence validated correctly", async () => {
      const target = (name: string) => ({
        schema: "df_integration_test",
        name,
        database: "dataform-integration-tests"
      });

      let evaluations = await dbadapter.evaluate(
        dataform.Table.create({
          type: "table",
          preOps: ["declare var string; set var = 'val';"],
          query: "select var as col;",
          target: target("example_valid_variable")
        })
      );
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      evaluations = await dbadapter.evaluate(
        dataform.Table.create({
          type: "table",
          query: "select var as col;",
          target: target("example_invalid_variable")
        })
      );
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE
      );
    });
  });

  suite("result limit works", async () => {
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
