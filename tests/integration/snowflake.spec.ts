import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { dropAllTables, getTableRows, keyBy } from "df/tests/integration/utils";

describe("@dataform/integration/snowflake", () => {
  const credentials = dfapi.credentials.read("snowflake", "df/test_credentials/snowflake.json");
  const dbadapter = dbadapters.create(credentials, "snowflake");

  it("run", async () => {
    const compiledGraph = await dfapi.compile({
      projectDir: "df/tests/integration/snowflake_project"
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

    // Check the status of the two assertions.
    expect(actionMap["DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_FAIL"].status).equals(
      dataform.ActionResult.ExecutionStatus.FAILED
    );
    expect(actionMap["DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_PASS"].status).equals(
      dataform.ActionResult.ExecutionStatus.SUCCESSFUL
    );

    // Check the status of the two uniqueness assertions.
    expect(
      actionMap["DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_UNIQUENESS_FAIL"].status
    ).equals(dataform.ActionResult.ExecutionStatus.FAILED);
    expect(
      actionMap["DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_UNIQUENESS_FAIL"].tasks[1]
        .errorMessage
    ).to.eql("snowflake error: Assertion failed: query returned 1 row(s).");
    expect(
      actionMap["DF_INTEGRATION_TEST_ASSERTIONS.EXAMPLE_ASSERTION_UNIQUENESS_PASS"].status
    ).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);

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

    expect(incrementalRows.length).equals(1);

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

    // Check there is an extra row in the incremental table.
    incrementalTable = keyBy(compiledGraph.tables, t => t.name)[
      "DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL"
    ];
    incrementalRows = await getTableRows(
      incrementalTable.target,
      adapter,
      credentials,
      "snowflake"
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
        expect(await dbadapter.execute(query, { interactive, maxResults: 2 })).eql([
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
});
