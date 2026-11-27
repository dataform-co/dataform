import { expect } from "chai";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import { CREDENTIALS_PATH } from "df/cli/index_test_base";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { integrationSuite } from "df/testing/credentials";
import { compile } from "df/tests/integration/utils";

integrationSuite("@dataform/integration/bigquery", { parallel: true }, () => {
  const credentials = dfapi.credentials.read(CREDENTIALS_PATH);
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
        messages: ["Expected 3 rows, but saw 2 rows."],
      },
      {
        name: "expected fewer columns than got",
        successful: false,
        messages: ['Expected columns "col1,col2,col3", but saw "col1,col2,col3,col4".'],
      },
      {
        name: "wrong columns",
        successful: false,
        messages: ['Expected columns "col1,col2,col3,col4", but saw "col1,col2,col3,col5".'],
      },
      {
        name: "wrong row contents",
        successful: false,
        messages: [
          'For row 0 and column "col2": expected "1", but saw "5".',
          'For row 1 and column "col3": expected "6.5", but saw "12".',
          'For row 2 and column "col1": expected "sup?", but saw "WRONG".',
        ],
      },
      { name: "test a view", successful: true },
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
        target: { schema: "", name: "", database: "" },
      };

      const bqadapter = new ExecutionSql({ warehouse: "bigquery" }, "1.4.8");

      const refresh = bqadapter.publishTasks(table, { fullRefresh: true }, { fields: [] }).build();
      const splitRefresh = refresh[0].statement.split("\n");
      expect([...splitRefresh.slice(0, 2), ...splitRefresh.slice(-2)]).to.eql([
        "preop task1;",
        "preop task2;",
        "postop task1;",
        "postop task2",
      ]);

      const increment = bqadapter
        .publishTasks(table, { fullRefresh: false }, { fields: [] })
        .build();

      const splitIncrement = increment[0].statement.split("\n");
      expect([...splitIncrement.slice(0, 2), ...splitIncrement.slice(-2)]).to.eql([
        "preop task1;",
        "preop task2;",
        "postop task1;",
        "postop task2",
      ]);
    });
  });

  suite("executeRaw", { parallel: true }, () => {
    const fiveRowsQuery = "SELECT x FROM UNNEST(GENERATE_ARRAY(1, 5)) AS x ORDER BY x";
    const rowValues = (rows: any[]) => rows.map((row) => row.f[0].v);

    test("returns at most rowLimit rows together with the schema", async () => {
      const result = await dbadapter.executeRaw(fiveRowsQuery, { rowLimit: 2 });

      expect(rowValues(result.rows)).eql(["1", "2"]);
      expect(result.schema.map((field) => field.name)).eql(["x"]);
    });

    test("returns rowLimit rows when they do not fit in the first page", async () => {
      // Each row is about 1 MB and a page holds about 10 MB.
      const largeRowsQuery =
        "SELECT x, REPEAT('x', 1000000) AS s FROM UNNEST(GENERATE_ARRAY(1, 15)) AS x ORDER BY x";

      const result = await dbadapter.executeRaw(largeRowsQuery, { rowLimit: 12 });

      expect(rowValues(result.rows)).eql(Array.from({ length: 12 }, (_, i) => String(i + 1)));
    });

    test("returns all rows when rowLimit is unset", async () => {
      const result = await dbadapter.executeRaw(fiveRowsQuery);

      expect(rowValues(result.rows)).eql(["1", "2", "3", "4", "5"]);
    });
  });
});
