import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

describe("@dataform/integration/snowflake", () => {
  it("run", async () => {
    const credentials = dfapi.credentials.read("snowflake", "df/test_profiles/snowflake.json");

    const compiledGraph = await dfapi.compile({
      projectDir: "df/tests/integration/snowflake_project"
    });

    const dbadapter = dbadapters.create(credentials, "snowflake");
    const adapter = adapters.create(compiledGraph.projectConfig);

    // Drop all the tables before we do anything.
    await dropAllTables(compiledGraph, adapter, dbadapter);

    // Run the project.
    let executionGraph = await dfapi.build(compiledGraph, {}, credentials);
    let executedGraph = await dfapi.run(executionGraph, credentials).resultPromise();

    const nodeMap = keyBy(executedGraph.nodes, v => v.name);

    // Check the status of the two tests.
    expect(nodeMap.example_assertion_fail.status).equals(dataform.NodeExecutionStatus.FAILED);
    expect(nodeMap.example_assertion_pass.status).equals(dataform.NodeExecutionStatus.SUCCESSFUL);

    // Check the data in the incremental table.
    let incrementalTable = keyBy(compiledGraph.tables, t => t.name).example_incremental;
    let incrementalRows = await getTableRows(incrementalTable.target, adapter, dbadapter);
    expect(incrementalRows.length).equals(1);

    // Re-run some of the actions.
    executionGraph = await dfapi.build(
      compiledGraph,
      { nodes: ["example_incremental", "example_table", "example_view"] },
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
