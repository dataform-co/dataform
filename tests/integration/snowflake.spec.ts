import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import * as adapters from "df/core/adapters";
import { SnowflakeAdapter } from "df/core/adapters/snowflake";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { compile, dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

process.env.SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT = "100";

suite("@dataform/integration/snowflake", ({ before, after }) => {
  const credentials = dfapi.credentials.read("snowflake", "test_credentials/snowflake.json");
  let dbadapter: dbadapters.IDbAdapter;

  before("create adapter", async () => {
    dbadapter = await dbadapters.create(credentials, "snowflake", { concurrencyLimit: 100 });
  });

  after("close adapter", () => dbadapter.close());

  test("run", { timeout: 90000 }, async () => {
    const compiledGraph = await compile("tests/integration/snowflake_project", "project_e2e");

    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);

    const tablesToDelete = (await dfapi.build(compiledGraph, {}, dbadapter)).warehouseState.tables;

    // Drop all the tables before we do anything.
    await dropAllTables(tablesToDelete, adapter, dbadapter);

    // Drop schemas to make sure schema creation works.
    await dbadapter.execute(
      `drop schema if exists "INTEGRATION_TESTS"."DF_INTEGRATION_TEST_PROJECT_E2E"`
    );
    await dbadapter.execute(
      `drop schema if exists "INTEGRATION_TESTS2"."DF_INTEGRATION_TEST_PROJECT_E2E"`
    );

    // Run the project.
    let executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    let executedGraph = await dfapi.run(dbadapter, executionGraph).result();

    const actionMap = keyBy(executedGraph.actions, v => targetAsReadableString(v.target));
    expect(Object.keys(actionMap).length).eql(17);

    // Check the status of action execution.
    const expectedFailedActions = [
      "INTEGRATION_TESTS.DF_INTEGRATION_TEST_ASSERTIONS_PROJECT_E2E.EXAMPLE_ASSERTION_FAIL"
    ];
    for (const actionName of Object.keys(actionMap)) {
      const expectedResult = expectedFailedActions.includes(actionName)
        ? dataform.ActionResult.ExecutionStatus.FAILED
        : dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
      expect(
        dataform.ActionResult.ExecutionStatus[actionMap[actionName].status],
        `ActionResult ExecutionStatus for action "${actionName}"` +
          ":" +
          actionMap[actionName].tasks.map(t => t.errorMessage)
      ).equals(dataform.ActionResult.ExecutionStatus[expectedResult]);
    }

    expect(
      actionMap[
        "INTEGRATION_TESTS.DF_INTEGRATION_TEST_ASSERTIONS_PROJECT_E2E.EXAMPLE_ASSERTION_FAIL"
      ].tasks[1].errorMessage
    ).to.eql("snowflake error: Assertion failed: query returned 1 row(s).");

    // Check the status of the view in the non-default database.
    const tada2DatabaseView = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
      "INTEGRATION_TESTS2.DF_INTEGRATION_TEST_PROJECT_E2E.SAMPLE_DATA_2"
    ];
    const tada2DatabaseViewRows = await getTableRows(tada2DatabaseView.target, adapter, dbadapter);
    expect(tada2DatabaseViewRows.length).equals(3);

    // Check the data in the incremental tables.
    let incrementalTable = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
      "INTEGRATION_TESTS.DF_INTEGRATION_TEST_PROJECT_E2E.EXAMPLE_INCREMENTAL"
    ];
    let incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(3);

    const incrementalTable2 = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
      "INTEGRATION_TESTS2.DF_INTEGRATION_TEST_PROJECT_E2E.EXAMPLE_INCREMENTAL_TADA2"
    ];
    const incrementalRows2 = await getTableRows(incrementalTable2.target, adapter, dbadapter);
    expect(incrementalRows2.length).equals(3);

    // Check the data in the incremental merge table.
    incrementalTable = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
      "INTEGRATION_TESTS.DF_INTEGRATION_TEST_PROJECT_E2E.EXAMPLE_INCREMENTAL_MERGE"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(2);

    // Re-run some of the actions.
    executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: [
          "EXAMPLE_INCREMENTAL",
          "EXAMPLE_INCREMENTAL_MERGE",
          "EXAMPLE_INCREMENTAL_TADA2",
          "EXAMPLE_TABLE",
          "EXAMPLE_VIEW"
        ]
      },
      dbadapter
    );

    executedGraph = await dfapi.run(dbadapter, executionGraph).result();
    expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Check there are the expected number of extra rows in the incremental tables.
    incrementalTable = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
      "INTEGRATION_TESTS.DF_INTEGRATION_TEST_PROJECT_E2E.EXAMPLE_INCREMENTAL"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(5);

    incrementalTable = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
      "INTEGRATION_TESTS2.DF_INTEGRATION_TEST_PROJECT_E2E.EXAMPLE_INCREMENTAL_TADA2"
    ];
    incrementalRows = await getTableRows(incrementalTable2.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(5);

    // Check the data in the incremental merge table.
    incrementalTable = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
      "INTEGRATION_TESTS.DF_INTEGRATION_TEST_PROJECT_E2E.EXAMPLE_INCREMENTAL_MERGE"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(2);
  });

  test("dataset metadata set correctly", { timeout: 60000 }, async () => {
    const compiledGraph = await compile("tests/integration/snowflake_project", "dataset_metadata");

    // Drop all the tables before we do anything.
    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);
    const tablesToDelete = (await dfapi.build(compiledGraph, {}, dbadapter)).warehouseState.tables;
    await dropAllTables(tablesToDelete, adapter, dbadapter);

    // Run the project.
    const executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["EXAMPLE_INCREMENTAL", "EXAMPLE_VIEW"],
        includeDependencies: true
      },
      dbadapter
    );
    const runResult = await dfapi.run(dbadapter, executionGraph).result();
    expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
      dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
    );

    // Check expected metadata.
    for (const expectedMetadata of [
      {
        target: {
          database: "INTEGRATION_TESTS",
          schema: "DF_INTEGRATION_TEST_DATASET_METADATA",
          name: "EXAMPLE_INCREMENTAL"
        },
        expectedDescription: "An incremental 'table'",
        expectedFields: [
          dataform.Field.create({
            description: "the id",
            name: "USER_ID",
            primitive: dataform.Field.Primitive.NUMERIC
          }),
          dataform.Field.create({
            description: "the 'timestamp'",
            name: "USER_TIMESTAMP",
            primitive: dataform.Field.Primitive.NUMERIC
          })
        ]
      },
      {
        target: {
          database: "INTEGRATION_TESTS",
          schema: "DF_INTEGRATION_TEST_DATASET_METADATA",
          name: "EXAMPLE_VIEW"
        },
        expectedDescription: "An example view",
        expectedFields: [
          dataform.Field.create({
            name: "VAL",
            primitive: dataform.Field.Primitive.NUMERIC
          })
        ]
      }
    ]) {
      const metadata = await dbadapter.table(expectedMetadata.target);
      expect(metadata.description).to.equal(expectedMetadata.expectedDescription);
      expect(
        metadata.fields.sort((fieldA, fieldB) => fieldA.name.localeCompare(fieldB.name))
      ).to.deep.equal(expectedMetadata.expectedFields);
    }
  });

  test("run unit tests", async () => {
    const compiledGraph = await compile("tests/integration/snowflake_project", "unit_tests");

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
          'For row 0 and column "COL2": expected "1", but saw "5".',
          'For row 1 and column "COL3": expected "6.5", but saw "12".',
          'For row 2 and column "COL1": expected "sup?", but saw "WRONG".'
        ]
      }
    ]);
  });

  suite("query limits work", async () => {
    const query = `
      select 1 union all
      select 2 union all
      select 3 union all
      select 4 union all
      select 5`;

    for (const options of [
      { interactive: true, rowLimit: 2 },
      { interactive: false, rowLimit: 2 },
      { interactive: true, byteLimit: 30 },
      { interactive: false, byteLimit: 30 }
    ]) {
      test(`with options=${JSON.stringify(options)}`, async () => {
        const { rows } = await dbadapter.execute(query, options);
        expect(rows).to.eql([
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

  suite("evaluate", async () => {
    test("evaluate from valid compiled graph as valid", { timeout: 60000 }, async () => {
      // Create and run the project.
      const compiledGraph = await compile("tests/integration/snowflake_project", "evaluate");
      const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
      await dfapi.run(dbadapter, executionGraph).result();

      const view = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
        "INTEGRATION_TESTS.DF_INTEGRATION_TEST_EVALUATE.EXAMPLE_VIEW"
      ];
      let evaluations = await dbadapter.evaluate(dataform.Table.create(view));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const assertion = keyBy(compiledGraph.assertions, t => targetAsReadableString(t.target))[
        "INTEGRATION_TESTS.DF_INTEGRATION_TEST_ASSERTIONS_EVALUATE.EXAMPLE_ASSERTION_PASS"
      ];
      evaluations = await dbadapter.evaluate(dataform.Assertion.create(assertion));
      expect(evaluations.length).to.equal(1);
      expect(evaluations[0].status).to.equal(
        dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      );

      const incremental = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target))[
        "INTEGRATION_TESTS.DF_INTEGRATION_TEST_EVALUATE.EXAMPLE_INCREMENTAL"
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
          enumType: dataform.TableType.TABLE,
          query: "selects\n1 as x",
          target: {
            name: "EXAMPLE_ILLEGAL_TABLE",
            database: "DF_INTEGRATION_TEST"
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
        enumType: dataform.TableType.INCREMENTAL,
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

  test("search", { timeout: 60000 }, async () => {
    // TODO: It seems as though, sometimes, the DB adapter can switch the current 'in-scope' database
    // away from 'INTEGRATION_TESTS' (the default) to 'INTEGRATION_TESTS2' (only used by one of the actions
    // in the graph). Re-creating a local DB adapter sucks, but forces queries to happen predictably against
    // 'INTEGRATION_TESTS'.
    const localDbAdapter = await dbadapters.create(credentials, "snowflake");

    const compiledGraph = await compile("tests/integration/snowflake_project", "search");

    // Drop all the tables before we do anything.
    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);
    const tablesToDelete = (await dfapi.build(compiledGraph, {}, localDbAdapter)).warehouseState
      .tables;
    await dropAllTables(tablesToDelete, adapter, localDbAdapter);

    // Run the project.
    const executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["EXAMPLE_VIEW"],
        includeDependencies: true
      },
      localDbAdapter
    );
    const runResult = await dfapi.run(localDbAdapter, executionGraph).result();
    expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
      dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
    );

    const [fullSearch, partialSearch, columnSearch] = await Promise.all([
      localDbAdapter.search("df_integration_test_search"),
      localDbAdapter.search("test_sear"),
      localDbAdapter.search("val")
    ]);

    expect(fullSearch.length).equals(2);
    expect(partialSearch.length).equals(2);
    expect(columnSearch.length).greaterThan(0);
  });
});
