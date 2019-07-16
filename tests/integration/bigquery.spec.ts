import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

describe("@dataform/integration/bigquery", () => {
  it("run", async () => {
    const credentials = dfapi.credentials.read("bigquery", "df/test_credentials/bigquery.json");

    const compiledGraph = await dfapi.compile({
      projectDir: "df/tests/integration/bigquery_project"
    });

    const dbadapter = dbadapters.create(credentials, "bigquery");
    const adapter = adapters.create(compiledGraph.projectConfig);

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

    const actionMap = keyBy(executedGraph.actions, v => v.name);

    // Check the status of the two assertions.
    expect(actionMap.example_assertion_fail.status).equals(dataform.ActionExecutionStatus.FAILED);
    expect(actionMap.example_assertion_pass.status).equals(
      dataform.ActionExecutionStatus.SUCCESSFUL
    );

    // Check the data in the incremental table.
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name).example_incremental;
    let incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(1);

    // Re-run some of the actions.
    executionGraph = await dfapi.build(
      compiledGraph,
      { actions: ["example_incremental", "example_table", "example_view"] },
      credentials
    );

    executedGraph = await dfapi.run(executionGraph, credentials).resultPromise();
    expect(executedGraph.ok).equals(true);

    // Check there is an extra row in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name).example_incremental;
    incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(2);
  }).timeout(60000);
});
