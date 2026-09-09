import { expect } from "chai";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { cleanWarehouse, compile } from "df/tests/integration/utils";

suite("@dataform/integration/bigquery", { parallel: true }, () => {
  const credentials = dfapi.credentials.read("test_credentials/bigquery.json");
  const dbadapter = new BigQueryDbAdapter(credentials);

  test("run unit tests", async () => {
    const compiledGraph = await compile("tests/integration/bigquery_project", "unit_tests");

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
      },
      { name: "test a view", successful: true }
    ]);
  });

  suite("publish tasks", { parallel: true }, async () => {
    test("incremental pre and post ops, core version <= 1.4.8", async () => {
      // 1.4.8 used `preOps` and `postOps` instead of `incrementalPreOps` and `incrementalPostOps`.
      const table: dataform.ITable = {
        enumType: dataform.TableType.INCREMENTAL,
        query: "query",
        preOps: ["preop task1", "preop task2"],
        incrementalQuery: "",
        postOps: ["postop task1", "postop task2"],
        target: { schema: "", name: "", database: "" }
      };

      const bqadapter = new ExecutionSql({ warehouse: "bigquery" }, "1.4.8");

      const refresh = bqadapter.publishTasks(table, { fullRefresh: true }, { fields: [] }).build();
      const splitRefresh = refresh[0].statement.split("\n");
      expect([...splitRefresh.slice(0, 2), ...splitRefresh.slice(-2)]).to.eql([
        "preop task1;",
        "preop task2;",
        "postop task1;",
        "postop task2"
      ]);

      const increment = bqadapter
        .publishTasks(table, { fullRefresh: false }, { fields: [] })
        .build();

      const splitIncrement = increment[0].statement.split("\n");
      expect([...splitIncrement.slice(0, 2), ...splitIncrement.slice(-2)]).to.eql([
        "preop task1;",
        "preop task2;",
        "postop task1;",
        "postop task2"
      ]);
    });
  });

  test("search", async () => {
    const compiledGraph = await compile("tests/integration/bigquery_project", "search");

    // Drop all the tables before we do anything.
    await cleanWarehouse(compiledGraph, dbadapter);

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
