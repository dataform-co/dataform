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

suite("@dataform/integration/bigquery", ({ before, after }) => {
  const credentials = dfapi.credentials.read("bigquery", "test_credentials/bigquery.json");
  let dbadapter: BigQueryDbAdapter;

  before("create adapter", async () => {
    dbadapter = (await dbadapters.create(credentials, "bigquery")) as BigQueryDbAdapter;
  });

  after("close adapter", () => dbadapter.close());

  test("run", { timeout: 60000 }, async () => {
    const compiledGraph = await dfapi.compile({
      projectDir: "tests/integration/bigquery_project"
    });

    expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);
    expect(compiledGraph.graphErrors.validationErrors).to.eql([]);

    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);

    // Drop all the tables before we do anything.
    const tablesToDelete = (await dfapi.build(compiledGraph, {}, dbadapter)).warehouseState.tables;
    await dropAllTables(tablesToDelete, adapter, dbadapter);

    // Drop schemas to make sure schema creation works.
    await dbadapter.dropSchema("dataform-integration-tests", "df_integration_test");

    // Drop the meta schema
    await dbadapter.dropSchema("dataform-integration-tests", "dataform_meta");

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

    // Run the project.
    let executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    let executedGraph = await dfapi.run(executionGraph, dbadapter).result();

    let actionMap = keyBy(executedGraph.actions, v => v.name);
    expect(Object.keys(actionMap).length).eql(14);

    // Check the status of action execution.
    const expectedFailedActions = [
      "dataform-integration-tests.df_integration_test_assertions.example_assertion_uniqueness_fail",
      "dataform-integration-tests.df_integration_test_assertions.example_assertion_fail"
    ];
    for (const actionName of Object.keys(actionMap)) {
      const expectedResult = expectedFailedActions.includes(actionName)
        ? dataform.ActionResult.ExecutionStatus.FAILED
        : dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
      expect(actionMap[actionName].status).equals(expectedResult);
    }

    expect(
      actionMap[
        "dataform-integration-tests.df_integration_test_assertions.example_assertion_uniqueness_fail"
      ].tasks[1].errorMessage
    ).to.eql("bigquery error: Assertion failed: query returned 1 row(s).");

    // Check the data in the incremental table.
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "dataform-integration-tests.df_integration_test.example_incremental"
    ];
    let incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(3);

    // Check the data in the incremental merge table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "dataform-integration-tests.df_integration_test.example_incremental_merge"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(2);

    // Re-run some of the actions.
    executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: [
          "example_incremental",
          "example_incremental_merge",
          "example_table",
          "example_view"
        ]
      },
      dbadapter
    );

    executedGraph = await dfapi.run(executionGraph, dbadapter).result();
    expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Check there are the expected number of extra rows in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "dataform-integration-tests.df_integration_test.example_incremental"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(5);

    // Check there are the expected number of extra rows in the incremental merge table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "dataform-integration-tests.df_integration_test.example_incremental_merge"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(2);

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
    actionMap = keyBy(executedGraph.actions, v => v.name);

    expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.FAILED);

    let expectedActionStatus: { [index: string]: dataform.ActionResult.ExecutionStatus } = {
      "dataform-integration-tests.df_integration_test.example_incremental":
        dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      "dataform-integration-tests.df_integration_test.example_incremental_merge":
        dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      "dataform-integration-tests.df_integration_test.example_table":
        dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED,
      "dataform-integration-tests.df_integration_test.example_view":
        dataform.ActionResult.ExecutionStatus.CACHE_SKIPPED,
      "dataform-integration-tests.df_integration_test_assertions.example_assertion_fail":
        dataform.ActionResult.ExecutionStatus.FAILED,
      "dataform-integration-tests.df_integration_test.example_operation":
        dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      "dataform-integration-tests.df_integration_test.depends_on_example_view":
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
      action => action.name === "dataform-integration-tests.df_integration_test.example_view"
    );
    expect(exampleView.definitionHash).to.eql(hashExecutionAction(exampleViewExecutionAction));

    const exampleAssertionFail = persistedMetaData.find(
      table => table.target.name === "example_assertion_fail"
    );
    expect(exampleAssertionFail).to.be.eql(undefined);

    compiledGraph.tables = compiledGraph.tables.map(table => {
      if (table.name === "dataform-integration-tests.df_integration_test.example_view") {
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
      "dataform-integration-tests.df_integration_test.example_view":
        dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      "dataform-integration-tests.df_integration_test.depends_on_example_view":
        dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    };

    for (const actionName of Object.keys(actionMap)) {
      expect(actionMap[actionName].status).equals(expectedActionStatus[actionName]);
    }

    // metadata
    await Promise.all([
      expectDatasetMetadata(
        dbadapter,
        {
          database: "dataform-integration-tests",
          schema: "df_integration_test",
          name: "example_incremental"
        },
        "An incremental table",
        EXPECTED_INCREMENTAL_EXAMPLE_SCHEMA
      ),
      expectDatasetMetadata(
        dbadapter,
        {
          database: "dataform-integration-tests",
          schema: "df_integration_test",
          name: "example_view"
        },
        "An example view",
        EXPECTED_EXAMPLE_VIEW_SCHEMA
      )
    ]);
  });

  suite("publish tasks", async () => {
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

  suite("execute", async () => {
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
  });
});

async function expectDatasetMetadata(
  dbadapter: BigQueryDbAdapter,
  target: dataform.ITarget,
  expectedDescription: string,
  expectedSchema: any
) {
  const incrementalMetadata = await dbadapter.getMetadata(target);
  expect(incrementalMetadata.description).to.equal(expectedDescription);
  expect(incrementalMetadata.schema).to.deep.equal(expectedSchema);
}
