import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { getTableRows, keyBy } from "df/tests/integration/utils";

describe("@dataform/integration/redshift", () => {
  it("run", async () => {
    const credentials = dfapi.credentials.read("redshift", "df/test_credentials/redshift.json");

    const compiledGraph = await dfapi.compile({
      projectDir: "df/tests/integration/redshift_project"
    });

    const dbadapter = dbadapters.create(credentials, "redshift");
    const adapter = adapters.create(compiledGraph.projectConfig);

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
    const testResults = await dfapi.test(compiledGraph, credentials);
    expect(testResults).to.eql([
      { name: "successful", successful: true },
      {
        name: "expected more rows than got",
        successful: false,
        message: "Expected 3 rows, but saw 2 rows."
      },
      {
        name: "expected fewer columns than got",
        successful: false,
        message: 'Expected columns "col1,col2,col3", but saw "col1,col2,col3,col4".'
      },
      {
        name: "wrong columns",
        successful: false,
        message: 'Expected columns "col1,col2,col3,col4", but saw "col1,col2,col3,col5".'
      },
      {
        name: "wrong row contents",
        successful: false,
        message: 'For row 2 and column "col1": expected "sup?", but saw "WRONG".'
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
