import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import * as adapters from "df/core/adapters";
import { SQLDataWarehouseAdapter } from "df/core/adapters/sqldatawarehouse";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { compile, dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

suite("@dataform/integration/sqldatawarehouse", { parallel: true }, ({ before, after }) => {
  const credentials = dfapi.credentials.read(
    "sqldatawarehouse",
    "test_credentials/sqldatawarehouse.json"
  );
  let dbadapter: dbadapters.IDbAdapter;

  before("create adapter", async () => {
    dbadapter = await dbadapters.create(credentials, "sqldatawarehouse");
  });
  after("close adapter", () => dbadapter.close());

  test("run", { timeout: 60000 }, async () => {
    const compiledGraph = await compile(
      "tests/integration/sqldatawarehouse_project",
      "project_e2e"
    );

    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);

    // Drop all the tables before we do anything.
    const tablesToDelete = (await dfapi.build(compiledGraph, {}, dbadapter)).warehouseState.tables;
    await dropAllTables(tablesToDelete, adapter, dbadapter);

    // Run the project.
    let executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    let executedGraph = await dfapi.run(dbadapter, executionGraph).result();

    const actionMap = keyBy(executedGraph.actions, v => v.name);
    expect(Object.keys(actionMap).length).eql(11);

    // Check the status of action execution.
    const expectedFailedActions = [
      "df_integration_test_assertions_project_e2e.example_assertion_uniqueness_fail",
      "df_integration_test_assertions_project_e2e.example_assertion_fail"
    ];
    for (const actionName of Object.keys(actionMap)) {
      const expectedResult = expectedFailedActions.includes(actionName)
        ? dataform.ActionResult.ExecutionStatus.FAILED
        : dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
      expect(actionMap[actionName].status).equals(expectedResult);
    }

    expect(
      actionMap["df_integration_test_assertions_project_e2e.example_assertion_uniqueness_fail"]
        .tasks[2].errorMessage
    ).to.eql("sqldatawarehouse error: Assertion failed: query returned 1 row(s).");

    // Check the data in the incremental table.
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test_project_e2e.example_incremental"
    ];
    let incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(1);

    // Re-run some of the actions.
    executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["example_incremental", "example_table", "example_view"]
      },
      dbadapter
    );

    executedGraph = await dfapi.run(dbadapter, executionGraph).result();
    expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Check there is an extra row in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test_project_e2e.example_incremental"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);

    expect(incrementalRows.length).equals(2);
  });

  test("run unit tests", async () => {
    const compiledGraph = await compile("tests/integration/sqldatawarehouse_project", "unit_tests");

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
          'For row 0 and column "col2": expected "1", but saw "5".',
          'For row 1 and column "col3": expected "6.5", but saw "12".',
          'For row 2 and column "col1": expected "sup?", but saw "WRONG".'
        ]
      }
    ]);
  });

  suite("query limits work", { parallel: true }, async () => {
    const query = `
      select 1 union all
      select 2 union all
      select 3 union all
      select 4 union all
      select 5`;

    for (const options of [
      { interactive: true, rowLimit: 2 },
      { interactive: false, rowLimit: 2 },
      { interactive: true, byteLimit: 20 },
      { interactive: false, byteLimit: 20 }
    ]) {
      test(`with options=${JSON.stringify(options)}`, async () => {
        const { rows } = await dbadapter.execute(query, options);
        expect(rows).to.eql([
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

  suite("evaluate", async () => {
    test("evaluate from valid compiled graph as valid", async () => {
      // Create and run the project.
      const compiledGraph = await compile("tests/integration/sqldatawarehouse_project", "evaluate");
      const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
      await dfapi.run(dbadapter, executionGraph).result();

      const view = keyBy(compiledGraph.tables, t => t.name)[
        "df_integration_test_evaluate.example_view"
      ];
      let evaluations = await dbadapter.evaluate(dataform.Table.create(view));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const table = keyBy(compiledGraph.tables, t => t.name)[
        "df_integration_test_evaluate.example_table"
      ];
      evaluations = await dbadapter.evaluate(dataform.Table.create(table));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const assertion = keyBy(compiledGraph.assertions, t => t.name)[
        "df_integration_test_assertions_evaluate.example_assertion_pass"
      ];
      evaluations = await dbadapter.evaluate(dataform.Assertion.create(assertion));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const incremental = keyBy(compiledGraph.tables, t => t.name)[
        "df_integration_test_evaluate.example_incremental"
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

    test("invalid table fails validation and error parsed correctly", async () => {
      const evaluations = await dbadapter.evaluate(
        dataform.Table.create({
          type: "table",
          query: "selects\n1 as x",
          target: {
            schema: "df_integration_test",
            name: "example_illegal_table",
            database: "dataform-integration-tests"
          }
        })
      );
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE
      );
      expect(
        dataform.QueryEvaluationError.ErrorLocation.create(evaluations[0].error.errorLocation)
      ).eql(dataform.QueryEvaluationError.ErrorLocation.create({ line: 1, column: 1 }));
    });
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

      const adapter = new SQLDataWarehouseAdapter({ warehouse: "sqldatawarehouse" }, "1.4.8");

      const refresh = adapter.publishTasks(table, { fullRefresh: true }, { fields: [] }).build();

      expect(refresh[0].statement).to.equal(table.preOps[0]);
      expect(refresh[1].statement).to.equal(table.preOps[1]);
      expect(refresh[refresh.length - 2].statement).to.equal(table.postOps[0]);
      expect(refresh[refresh.length - 1].statement).to.equal(table.postOps[1]);

      const increment = adapter.publishTasks(table, { fullRefresh: false }, { fields: [] }).build();

      expect(increment[0].statement).to.equal(table.preOps[0]);
      expect(increment[1].statement).to.equal(table.preOps[1]);
      expect(increment[increment.length - 2].statement).to.equal(table.postOps[0]);
      expect(increment[increment.length - 1].statement).to.equal(table.postOps[1]);
    });
  });

  test("search", async () => {
    const compiledGraph = await compile("tests/integration/sqldatawarehouse_project", "search");

    // Drop all the tables before we do anything.
    const tablesToDelete = (await dfapi.build(compiledGraph, {}, dbadapter)).warehouseState.tables;
    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);
    await dropAllTables(tablesToDelete, adapter, dbadapter);

    // Run the project.
    const executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["example_view"],
        includeDependencies: true
      },
      dbadapter
    );
    const runResult = await dfapi.run(dbadapter, executionGraph).result();
    expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
      dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
    );

    const [fullSearch, partialSearch, columnSearch] = await Promise.all([
      dbadapter.search("df_integration_test_search"),
      dbadapter.search("test_sear"),
      dbadapter.search("val")
    ]);

    expect(fullSearch.length).equals(2);
    expect(partialSearch.length).equals(2);
    expect(columnSearch.length).greaterThan(0);
  });
});
