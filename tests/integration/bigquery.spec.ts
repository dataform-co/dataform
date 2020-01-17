import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { BigQueryAdapter } from "df/core/adapters/bigquery";
import { dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";
import * as Long from "long";

describe("@dataform/integration/bigquery", () => {
  const credentials = dfapi.credentials.read("bigquery", "df/test_credentials/bigquery.json");
  const dbadapter = dbadapters.create(credentials, "bigquery");
  after(() => dbadapter.close());

  it("run", async () => {
    const compiledGraph = await dfapi.compile({
      projectDir: "df/tests/integration/bigquery_project"
    });

    expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);
    expect(compiledGraph.graphErrors.validationErrors).to.eql([]);

    const adapter = adapters.create(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion);

    // Drop all the tables before we do anything.
    await dropAllTables(compiledGraph, adapter, dbadapter);

    // Run the tests.
    const testResults = await dfapi.test(credentials, "bigquery", compiledGraph.tests);
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

    console.log("PRE ACTION MAP");

    const actionMap = keyBy(executedGraph.actions, v => v.name);

    // Check the status of the two assertions.
    expect(
      actionMap["dataform-integration-tests.df_integration_test_assertions.example_assertion_fail"]
        .status
    ).equals(dataform.ActionResult.ExecutionStatus.FAILED);
    expect(
      actionMap["dataform-integration-tests.df_integration_test_assertions.example_assertion_pass"]
        .status
    ).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);

    // Check the status of the two uniqueness assertions.
    expect(
      actionMap[
        "dataform-integration-tests.df_integration_test_assertions.example_assertion_uniqueness_fail"
      ].status
    ).equals(dataform.ActionResult.ExecutionStatus.FAILED);
    expect(
      actionMap[
        "dataform-integration-tests.df_integration_test_assertions.example_assertion_uniqueness_fail"
      ].tasks[1].errorMessage
    ).to.eql("bigquery error: Assertion failed: query returned 1 row(s).");
    expect(
      actionMap[
        "dataform-integration-tests.df_integration_test_assertions.example_assertion_uniqueness_pass"
      ].status
    ).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);

    console.log(
      "ACTION MAP",
      actionMap["dataform-integration-tests.df_integration_test.example_incremental"]
    );

    // Check the status of tests expected to pass.
    expect(
      actionMap["dataform-integration-tests.df_integration_test.example_incremental"].status
    ).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);
    expect(actionMap["dataform-integration-tests.df_integration_test.example_table"].status).equals(
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    );
    expect(actionMap["dataform-integration-tests.df_integration_test.example_view"].status).equals(
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    );
    expect(actionMap["dataform-integration-tests.df_integration_test.sample_data_2"].status).equals(
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    );
    expect(actionMap["dataform-integration-tests.df_integration_test.sample_data"].status).equals(
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    );

    console.log("FINDING INCREMENTAL BY KEY");
    compiledGraph.tables.forEach(table => console.log("TABLE NAME:", table.name));

    // Check the data in the incremental table.
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "dataform-integration-tests.df_integration_test.example_incremental"
    ];

    console.log("FOUND BY KEY, INCREMENTALTABLE", incrementalTable);

    let incrementalRows = await getTableRows(
      incrementalTable.target,
      adapter,
      credentials,
      "bigquery"
    );

    console.log("INCREMENTAL ROWS:", incrementalRows);

    expect(incrementalRows.length).equals(1);

    // Re-run some of the actions.
    executionGraph = await dfapi.build(
      compiledGraph,
      {
        actions: ["example_incremental", "example_table", "example_view"]
      },
      credentials
    );

    console.log("EXECUTION GRAPH", executionGraph);

    executedGraph = await dfapi.run(executionGraph, credentials).resultPromise();
    expect(executedGraph.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    // Check there is an extra row in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "dataform-integration-tests.df_integration_test.example_incremental"
    ];
    incrementalRows = await getTableRows(incrementalTable.target, adapter, credentials, "bigquery");
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

  describe("publish tasks", async () => {
    const projectConfig: dataform.IProjectConfig = {
      warehouse: "bigquery",
      defaultDatabase: "default_database"
    };

    const templateTable: dataform.ITable = {
      type: "incremental",
      incrementalQuery: "incrementalQuery",
      target: {
        schema: "df_integration_test",
        name: "example_incremental",
        database: "dataform-integration-tests"
      }
    };

    const refreshRunConfig: dataform.IRunConfig = {
      fullRefresh: true
    };

    const noRefreshRunConfig: dataform.IRunConfig = {
      fullRefresh: false
    };

    const tableMetadata: dataform.ITableMetadata = {
      fields: []
    };

    const expectedRefreshStatements = [
      "preop task1",
      "preop task2",
      "drop view if exists `dataform-integration-tests.df_integration_test.example_incremental`",
      "create or replace table `dataform-integration-tests.df_integration_test.example_incremental`  as incrementalQuery",
      "postop task1",
      "postop task2"
    ];

    const expectedIncrementStatements = [
      "preop task1",
      "preop task2",
      "drop view if exists `dataform-integration-tests.df_integration_test.example_incremental`",
      `
      insert into \`dataform-integration-tests.df_integration_test.example_incremental\`
      ()
      select 
      from (select * from (incrementalQuery) as subquery
        where true) as insertions`,
      "postop task1",
      "postop task2"
    ];

    it("incremental, core version < 1.4.8", async () => {
      const bqadapter = new BigQueryAdapter(projectConfig, "1.4.7");
      const table = { ...templateTable };
      table.preOps = ["preop task1", "preop task2"];
      table.postOps = ["postop task1", "postop task2"];

      const buildsFromRefresh = bqadapter
        .publishTasks(table, refreshRunConfig, tableMetadata)
        .build();

      buildsFromRefresh.forEach((build, i) => {
        expect(build.statement).to.eql(expectedRefreshStatements[i]);
      });

      const buildsFromIncrement = bqadapter
        .publishTasks(table, noRefreshRunConfig, tableMetadata)
        .build();

      buildsFromIncrement.forEach((build, i) => {
        expect(build.statement).to.eql(expectedIncrementStatements[i]);
      });
    });

    it("incremental, core version >= 1.4.8", async () => {
      const bqadapter = new BigQueryAdapter(projectConfig, "1.4.8");
      const table = { ...templateTable };
      table.incrementalPreOps = ["preop task1", "preop task2"];
      table.incrementalPostOps = ["postop task1", "postop task2"];

      const buildsFromRefresh = bqadapter
        .publishTasks(table, refreshRunConfig, tableMetadata)
        .build();

      buildsFromRefresh.forEach((build, i) => {
        expect(build.statement).to.eql(expectedRefreshStatements[i]);
      });

      const buildsFromIncrement = bqadapter
        .publishTasks(table, noRefreshRunConfig, tableMetadata)
        .build();

      buildsFromIncrement.forEach((build, i) => {
        expect(build.statement).to.eql(expectedIncrementStatements[i]);
      });
    });
  });

  describe("metadata", async () => {
    it("includes jobReference and statistics", async () => {
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
  });
});
