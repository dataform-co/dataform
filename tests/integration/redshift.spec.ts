import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { RedshiftAdapter } from "df/core/adapters/redshift";
import { suite, test } from "df/testing";
import { getTableRows, keyBy } from "df/tests/integration/utils";

suite("@dataform/integration/redshift", ({ after }) => {
  const credentials = dfapi.credentials.read("redshift", "test_credentials/redshift.json");
  const dbadapter = dbadapters.create(credentials, "redshift");
  after("close adapter", () => dbadapter.close());

  test("run", { timeout: 60000 }, async () => {
    const compiledGraph = await dfapi.compile({
      projectDir: "tests/integration/redshift_project"
    });

    expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);
    expect(compiledGraph.graphErrors.validationErrors).to.eql([]);

    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);

    // Redshift transactions are giving us headaches here. Drop tables sequentially.
    const dropFunctions = [].concat(
      compiledGraph.tables.map(table => () =>
        dbadapter.execute(adapter.dropIfExists(table.target, adapter.baseTableType(table.type)))
      ),
      compiledGraph.assertions.map(assertion => () =>
        dbadapter.execute(adapter.dropIfExists(assertion.target, "view"))
      )
    );
    await dropFunctions.reduce((promiseChain, fn) => promiseChain.then(fn), Promise.resolve());

    // Run the tests.
    const testResults = await dfapi.test(credentials, "redshift", compiledGraph.tests);
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
          'For row 1 and column "col3": expected "6.5" (string), but saw "12.0" (string).',
          'For row 2 and column "col1": expected "sup?" (string), but saw "WRONG" (string).'
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
        "df_integration_test_assertions.example_assertion_pass",
        "df_integration_test_assertions.example_assertion_uniqueness_pass",
        "df_integration_test.example_incremental",
        "df_integration_test.example_table",
        "df_integration_test.example_view",
        "df_integration_test.load_from_s3",
        "df_integration_test.sample_data_2",
        "df_integration_test.sample_data"
      ],
      failed: [
        "df_integration_test_assertions.example_assertion_uniqueness_fail",
        "df_integration_test_assertions.example_assertion_fail"
      ]
    };

    expectedRunStatuses.successful.forEach(actionName =>
      expect(actionMap[actionName].status).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL)
    );

    expectedRunStatuses.failed.forEach(actionName =>
      expect(actionMap[actionName].status).equals(dataform.ActionResult.ExecutionStatus.FAILED)
    );

    expect(
      actionMap["df_integration_test_assertions.example_assertion_uniqueness_fail"].tasks[1]
        .errorMessage
    ).to.eql("redshift error: Assertion failed: query returned 1 row(s).");

    // Check the status of the s3 load operation.
    expect(actionMap["df_integration_test.load_from_s3"].status).equals(
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    );

    // Check the s3 table has two rows, as per:
    // https://dataform-integration-tests.s3.us-east-2.amazonaws.com/sample-data/sample_data.csv
    const s3Table = keyBy(compiledGraph.operations, t => t.name)[
      "df_integration_test.load_from_s3"
    ];
    const s3Rows = await getTableRows(s3Table.target, adapter, credentials, "redshift");
    expect(s3Rows.length).equals(2);

    // Check the data in the incremental table.
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test.example_incremental"
    ];
    let incrementalRows = await getTableRows(
      incrementalTable.target,
      adapter,
      credentials,
      "redshift"
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
    executedGraph = await dfapi.run(executionGraph, credentials).resultPromise();
    expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Check there is an extra row in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "df_integration_test.example_incremental"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, credentials, "redshift");
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
            "?column?": 1
          },
          {
            "?column?": 2
          }
        ]);
      });
    }
  });

  suite("publish tasks", async () => {
    test("incremental, core version <= 1.4.8", async () => {
      const projectConfig: dataform.IProjectConfig = {
        warehouse: "bigquery",
        defaultDatabase: "default_database"
      };

      const table: dataform.ITable = {
        type: "incremental",
        query: "query",
        preOps: ["preop task1", "preop task2"],
        incrementalQuery: "query where incremental",
        postOps: ["postop task1", "postop task2"],
        target: {
          schema: "df_integration_test",
          name: "example_incremental",
          database: "dataform-integration-tests"
        }
      };

      const expectedRefreshStatements = [
        table.preOps[0],
        table.preOps[1],
        `drop view if exists "${table.target.schema}"."${table.target.name}" cascade`,
        `drop table if exists "${table.target.schema}"."${table.target.name}_temp" cascade`,
        `create table "${table.target.schema}"."${table.target.name}_temp" as query`,
        `drop table if exists "${table.target.schema}"."${table.target.name}" cascade`,
        `alter table "${table.target.schema}"."${table.target.name}_temp" rename to "${table.target.name}"`,
        table.postOps[0],
        table.postOps[1]
      ];

      const expectedIncrementStatements = [
        table.preOps[0],
        table.preOps[1],
        `drop view if exists "${table.target.schema}"."${table.target.name}" cascade`,
        `
insert into "${table.target.schema}"."${table.target.name}"
()
select 
from (
  select * from (${table.incrementalQuery}) as subquery
    where true) as insertions`,
        table.postOps[0],
        table.postOps[1]
      ];

      const bqadapter = new RedshiftAdapter(projectConfig, "1.4.8");

      const buildsFromRefresh = bqadapter
        .publishTasks(table, { fullRefresh: true }, { fields: [] })
        .build();

      buildsFromRefresh.forEach((build, i) => {
        expect(build.statement).to.eql(expectedRefreshStatements[i]);
      });

      const buildsFromIncrement = bqadapter
        .publishTasks(table, { fullRefresh: false }, { fields: [] })
        .build();

      buildsFromIncrement.forEach((build, i) => {
        expect(build.statement).to.eql(expectedIncrementStatements[i]);
      });
    });
  });
});
