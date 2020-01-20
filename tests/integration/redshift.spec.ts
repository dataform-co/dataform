import * as dfapi from "@dataform/api";
import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
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
      actionMap["df_integration_test_assertions.example_assertion_uniqueness_fail"].tasks[1]
        .errorMessage
    ).to.eql("redshift error: Assertion failed: query returned 1 row(s).");
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
        expect(await dbadapter.execute(query, { interactive, maxResults: 2 })).eql([
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
});
