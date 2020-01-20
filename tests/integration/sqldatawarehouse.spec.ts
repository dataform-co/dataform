import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { SQLDataWarehouseAdapter } from "df/core/adapters/sqldatawarehouse";
import { dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

describe("@dataform/integration/sqldatawarehouse", () => {
  const credentials = dfapi.credentials.read(
    "sqldatawarehouse",
    "df/test_credentials/sqldatawarehouse.json"
  );
  const dbadapter = dbadapters.create(credentials, "sqldatawarehouse");
  after(() => dbadapter.close());

  it("run", async () => {
    const compiledGraph = await dfapi.compile({
      projectDir: "df/tests/integration/sqldatawarehouse_project"
    });

    expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);
    expect(compiledGraph.graphErrors.validationErrors).to.eql([]);

    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);

    // Drop all the tables before we do anything.
    await dropAllTables(compiledGraph, adapter, dbadapter);

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
    let executedGraph = await dfapi.run(executionGraph, credentials).resultPromise();

    const actionMap = keyBy(executedGraph.actions, v => v.name);

    // Check the status of the two assertions.
    expect(actionMap["df_integration_test_assertions.example_assertion_fail"].status).equals(
      dataform.ActionResult.ExecutionStatus.FAILED
    );
    expect(actionMap["df_integration_test_assertions.example_assertion_pass"].status).equals(
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    );

    // Check the status of the two uniqueness assertions.
    expect(
      actionMap["df_integration_test_assertions.example_assertion_uniqueness_fail"].status
    ).equals(dataform.ActionResult.ExecutionStatus.FAILED);
    expect(
      actionMap["df_integration_test_assertions.example_assertion_uniqueness_fail"].tasks[2]
        .errorMessage
    ).to.eql("sqldatawarehouse error: Assertion failed: query returned 1 row(s).");
    expect(
      actionMap["df_integration_test_assertions.example_assertion_uniqueness_pass"].status
    ).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);

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

    executedGraph = await dfapi.run(executionGraph, credentials).resultPromise();
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
  }).timeout(60000);

  describe("result limit works", async () => {
    const query = `
      select 1 union all
      select 2 union all
      select 3 union all
      select 4 union all
      select 5`;

    for (const interactive of [true, false]) {
      it(`with interactive=${interactive}`, async () => {
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

  describe("publish tasks", async () => {
    it("incremental, core version <= 1.4.8", async () => {
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
        `drop view if exists "${table.target.schema}"."${table.target.name}" `,
        `if object_id ('"${table.target.schema}"."${table.target.name}_temp"','U') is not null drop table "${table.target.schema}"."${table.target.name}_temp"`,
        `create table "${table.target.schema}"."${table.target.name}_temp"
     with(
       distribution = ROUND_ROBIN
     ) 
     as query`,
        `if object_id ('"${table.target.schema}"."${table.target.name}"','U') is not null drop table "${table.target.schema}"."${table.target.name}"`,
        `rename object "${table.target.schema}"."${table.target.name}_temp" to ${table.target.name} `,
        table.postOps[0],
        table.postOps[1]
      ];

      const expectedIncrementStatements = [
        table.preOps[0],
        table.preOps[1],
        `drop view if exists "${table.target.schema}"."${table.target.name}" `,
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

      const bqadapter = new SQLDataWarehouseAdapter(projectConfig, "1.4.8");

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
