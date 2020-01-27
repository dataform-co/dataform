import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { SnowflakeAdapter } from "df/core/adapters/snowflake";
import { suite, test } from "df/testing";
import { dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

suite("@dataform/integration/snowflake", ({ after }) => {
  const credentials = dfapi.credentials.read("snowflake", "test_credentials/snowflake.json");
  const dbadapter = dbadapters.create(credentials, "snowflake");
  after("close adapter", () => dbadapter.close());

  test("run", { timeout: 60000 }, async () => {
    const compiledGraph = await dfapi.compile({
      projectDir: "tests/integration/snowflake_project"
    });

    expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);
    expect(compiledGraph.graphErrors.validationErrors).to.eql([]);

    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);

    // Drop all the tables before we do anything.
    await dropAllTables(compiledGraph, adapter, dbadapter);

    // Run the tests.
    const testResults = await dfapi.test(credentials, "snowflake", compiledGraph.tests);
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
        messages: ['Expected columns "COL1,COL2,COL3", but saw "COL1,COL2,COL3,COL4".']
      },
      {
        name: "wrong columns",
        successful: false,
        messages: ['Expected columns "COL1,COL2,COL3,COL4", but saw "COL1,COL2,COL3,COL5".']
      },
      {
        name: "wrong row contents",
        successful: false,
        messages: [
          'For row 0 and column "COL2": expected "1" (number), but saw "5" (number).',
          'For row 1 and column "COL3": expected "6.5" (number), but saw "12" (number).',
          'For row 2 and column "COL1": expected "sup?" (string), but saw "WRONG" (string).'
        ]
      }
    ]);

    // Run the project.
    let executionGraph = await dfapi.build(compiledGraph, {}, credentials);
    let executedGraph = await dfapi.run(executionGraph, credentials).resultPromise();

    const actionMap = keyBy(executedGraph.actions, v => v.name);

    // Check the status of file execution.
    const expectedRunStatuses = {
      successful: [
        "DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_PASS",
        "DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_UNIQUENESS_PASS",
        "DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL",
        "DF_INTEGRATION_TEST.EXAMPLE_TABLE",
        "DF_INTEGRATION_TEST.EXAMPLE_VIEW",
        "DF_INTEGRATION_TEST.LOAD_FROM_S3",
        "TADA2.DF_INTEGRATION_TEST.SAMPLE_DATA_2",
        "DF_INTEGRATION_TEST.SAMPLE_DATA"
      ],
      failed: [
        "DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_UNIQUENESS_FAIL",
        "DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_FAIL"
      ]
    };

    expectedRunStatuses.successful.forEach(actionName =>
      expect(actionMap[actionName].status).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL)
    );

    expectedRunStatuses.failed.forEach(actionName =>
      expect(actionMap[actionName].status).equals(dataform.ActionResult.ExecutionStatus.FAILED)
    );

    expect(
      actionMap["DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_UNIQUENESS_FAIL"].tasks[1]
        .errorMessage
    ).to.eql("snowflake error: Assertion failed: query returned 1 row(s).");

    // Check the status of the s3 load operation.
    expect(actionMap["DF_INTEGRATION_TEST.LOAD_FROM_S3"].status).equals(
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    );

    // Check the s3 table has two rows, as per:
    // https://dataform-integration-tests.s3.us-east-2.amazonaws.com/sample-data/sample_data.csv
    const s3Table = keyBy(compiledGraph.operations, t => t.name)[
      "DF_INTEGRATION_TEST.LOAD_FROM_S3"
    ];
    const s3Rows = await getTableRows(s3Table.target, adapter, credentials, "snowflake");
    expect(s3Rows.length).equals(2);

    // Check the status of the view in the non-default database.
    const tada2DatabaseView = keyBy(compiledGraph.tables, t => t.name)[
      "TADA2.DF_INTEGRATION_TEST.SAMPLE_DATA_2"
    ];
    const tada2DatabaseViewRows = await getTableRows(
      tada2DatabaseView.target,
      adapter,
      credentials,
      "snowflake"
    );
    expect(tada2DatabaseViewRows.length).equals(3);

    // Check the data in the incremental table.
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL"
    ];
    let incrementalRows = await getTableRows(
      incrementalTable.target,
      adapter,
      credentials,
      "snowflake"
    );
    expect(incrementalRows.length).equals(3);

    // Re-run some of the actions.
    executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["EXAMPLE_INCREMENTAL", "EXAMPLE_TABLE", "EXAMPLE_VIEW"]
      },
      credentials
    );

    executedGraph = await dfapi.run(executionGraph, credentials).resultPromise();
    expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Check there are the expected number of extra rows in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL"
    ];
    incrementalRows = await getTableRows(
      incrementalTable.target,
      adapter,
      credentials,
      "snowflake"
    );
    expect(incrementalRows.length).equals(5);
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
            1: 1
          },
          {
            1: 2
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

      const bqadapter = new SnowflakeAdapter({ warehouse: "snowflake" }, "1.4.8");

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
