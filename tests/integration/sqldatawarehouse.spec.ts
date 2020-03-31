import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { SQLDataWarehouseAdapter } from "df/core/adapters/sqldatawarehouse";
import { suite, test } from "df/testing";
import { dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

suite("@dataform/integration/sqldatawarehouse", ({ after }) => {
  const credentials = dfapi.credentials.read(
    "sqldatawarehouse",
    "test_credentials/sqldatawarehouse.json"
  );
  const dbadapter = dbadapters.create(credentials, "sqldatawarehouse");
  after("close adapter", () => dbadapter.close());

  test("run", { timeout: 60000 }, async () => {
    const compiledGraph = await dfapi.compile({
      projectDir: "tests/integration/sqldatawarehouse_project"
    });

    expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);
    expect(compiledGraph.graphErrors.validationErrors).to.eql([]);

    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);

    // Drop all the tables before we do anything.
    const tablesToDelete = (await dfapi.build(compiledGraph, {}, credentials)).warehouseState
      .tables;
    await dropAllTables(tablesToDelete, adapter, dbadapter);

    // Run the tests.
    const testResults = await dfapi.test(credentials, "sqldatawarehouse", compiledGraph.tests);
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
    let executionGraph = await dfapi.build(compiledGraph, {}, credentials);
    let executedGraph = await dfapi.run(executionGraph, credentials).result();

    const actionMap = keyBy(executedGraph.actions, v => v.name);
    expect(Object.keys(actionMap).length).eql(11);

    // Check the status of action execution.
    const expectedFailedActions = [
      "df_integration_test_assertions.example_assertion_uniqueness_fail",
      "df_integration_test_assertions.example_assertion_fail"
    ];
    for (const actionName of Object.keys(actionMap)) {
      const expectedResult = expectedFailedActions.includes(actionName)
        ? dataform.ActionResult.ExecutionStatus.FAILED
        : dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
      expect(actionMap[actionName].status, JSON.stringify(executionGraph, null, 4)).equals(
        expectedResult
      );
    }

    expect(
      actionMap["df_integration_test_assertions.example_assertion_uniqueness_fail"].tasks[2]
        .errorMessage
    ).to.eql("sqldatawarehouse error: Assertion failed: query returned 1 row(s).");

    // Check the data in the incremental table.
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test.example_incremental"
    ];
    let incrementalRows = await getTableRows(
      incrementalTable.target,
      adapter,
      credentials,
      "sqldatawarehouse"
    );
    expect(incrementalRows.length).equals(1);

    // Re-run some of the actions.
    executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["example_incremental", "example_table", "example_view"]
      },
      credentials
    );

    executedGraph = await dfapi.run(executionGraph, credentials).result();
    expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Check there is an extra row in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test.example_incremental"
    ];
    incrementalRows = await getTableRows(
      incrementalTable.target,
      adapter,
      credentials,
      "sqldatawarehouse"
    );

    expect(incrementalRows.length).equals(2);
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
        expect(rows).eql([
          {
            "": 1
          },
          {
            "": 2
          }
        ]);
      });
    }
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

      const bqadapter = new SQLDataWarehouseAdapter({ warehouse: "sqldatawarehouse" }, "1.4.8");

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
});
