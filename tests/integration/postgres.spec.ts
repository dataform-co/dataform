import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import * as adapters from "df/core/adapters";
import { RedshiftAdapter } from "df/core/adapters/redshift";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { compile, getTableRows, keyBy } from "df/tests/integration/utils";
import { PostgresFixture } from "df/tools/postgres/postgres_fixture";

suite("@dataform/integration/postgres", { parallel: true }, ({ before, after }) => {
  let dbadapter: dbadapters.IDbAdapter;

  const postgres = new PostgresFixture(5432, before, after);

  before("create adapter", async () => {
    dbadapter = await dbadapters.create(
      {
        username: "postgres",
        databaseName: "postgres",
        password: "password",
        port: 5432,
        host: PostgresFixture.host
      },
      "postgres",
      { disableSslForTestsOnly: true }
    );
  });

  test("run", { timeout: 60000 }, async () => {
    const compiledGraph = await compile("tests/integration/postgres_project", "project_e2e");

    // Run the project.
    let executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    let executedGraph = await dfapi.run(dbadapter, executionGraph).result();

    const actionMap = keyBy(executedGraph.actions, v => v.name);
    expect(Object.keys(actionMap).length).eql(12);

    // Check the status of action execution.
    const expectedFailedActions = [
      "df_integration_test_assertions_project_e2e.example_assertion_uniqueness_fail",
      "df_integration_test_assertions_project_e2e.example_assertion_fail"
    ];
    for (const actionName of Object.keys(actionMap)) {
      const expectedResult = expectedFailedActions.includes(actionName)
        ? dataform.ActionResult.ExecutionStatus.FAILED
        : dataform.ActionResult.ExecutionStatus.SUCCESSFUL;
      expect(actionMap[actionName].status).equals(
        expectedResult,
        actionMap[actionName].tasks.map(task => task.errorMessage).join("\n")
      );
    }

    expect(
      actionMap["df_integration_test_assertions_project_e2e.example_assertion_uniqueness_fail"]
        .tasks[2].errorMessage
    ).to.eql("postgres error: Assertion failed: query returned 1 row(s).");

    // Check the data in the incremental table.
    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test_project_e2e.example_incremental"
    ];
    let incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(3);

    // Check the data in the incremental merge table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test_project_e2e.example_incremental_merge"
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
    executedGraph = await dfapi.run(dbadapter, executionGraph).result();
    expect(executedGraph.status).equals(
      dataform.RunResult.ExecutionStatus.SUCCESSFUL,
      executedGraph.actions
        .map(action => action.tasks.map(task => task.errorMessage).join("\n"))
        .join("\n")
    );

    // Check there are the expected number of extra rows in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test_project_e2e.example_incremental"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(5);

    // Check there are the expected number of extra rows in the incremental merge table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test_project_e2e.example_incremental_merge"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(2);
  });

  test("dataset metadata set correctly", { timeout: 60000 }, async () => {
    const compiledGraph = await compile("tests/integration/postgres_project", "dataset_metadata");

    // Run the project.
    const executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["example_incremental", "example_view"],
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
          schema: "df_integration_test_dataset_metadata",
          name: "example_incremental"
        },
        expectedDescription: "An incremental 'table'",
        expectedFields: [
          dataform.Field.create({
            description: "the 'timestamp'",
            flagsDeprecated: ["nullable"],
            name: "user_timestamp",
            primitive: dataform.Field.Primitive.INTEGER,
            primitiveDeprecated: "integer"
          }),
          dataform.Field.create({
            description: "the id",
            flagsDeprecated: ["nullable"],
            name: "user_id",
            primitive: dataform.Field.Primitive.INTEGER,
            primitiveDeprecated: "integer"
          })
        ]
      },
      {
        target: {
          schema: "df_integration_test_dataset_metadata",
          name: "example_view"
        },
        expectedDescription: "An example view",
        expectedFields: [
          dataform.Field.create({
            name: "val",
            description: "val doc",
            flagsDeprecated: ["nullable"],
            primitive: dataform.Field.Primitive.INTEGER,
            primitiveDeprecated: "integer"
          })
        ]
      }
    ]) {
      const metadata = await dbadapter.table(expectedMetadata.target);
      expect(metadata.description).to.equal(expectedMetadata.expectedDescription);
      expect(metadata.fields).to.deep.equal(expectedMetadata.expectedFields);
    }
  });

  test("run unit tests", async () => {
    const compiledGraph = await compile("tests/integration/postgres_project", "unit_tests");

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
      { interactive: true, byteLimit: 50 },
      { interactive: false, byteLimit: 50 }
    ]) {
      test(`with options=${JSON.stringify(options)}`, async () => {
        const { rows } = await dbadapter.execute(query, options);
        expect(rows).to.eql([
          {
            "?column?": 1
          },
          {
            "?column?": 2
          }
        ]);
      });
    }
  });

  suite("evaluate", () => {
    test("evaluate from valid compiled graph as valid", async () => {
      const compiledGraph = await compile("tests/integration/postgres_project", "evaluate");
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

    test("invalid table fails validation", async () => {
      const evaluations = await dbadapter.evaluate(
        dataform.Table.create({
          type: "table",
          query: "thisisillegal",
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

      const adapter = new RedshiftAdapter({ warehouse: "postgres" }, "1.4.8");

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
    const compiledGraph = await compile("tests/integration/postgres_project", "search");

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

    expect((await dbadapter.search("df_integration_test_search")).length).equals(2);
    expect((await dbadapter.search("test_sear")).length).equals(2);
    expect((await dbadapter.search("val")).length).greaterThan(0);
  });
});
