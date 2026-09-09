import { expect } from "chai";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { INTEGRATION_TEST_PROJECT } from "df/cli/index_test_base";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { compile, keyBy } from "df/tests/integration/utils";

suite("@dataform/integration/evaluate", () => {
  const credentials = dfapi.credentials.read("test_credentials/bigquery.json");
  const dbadapter = new BigQueryDbAdapter(credentials);

  test("evaluate from valid compiled graph as valid", async () => {
    // Create and run the project.
    const compiledGraph = await compile("tests/integration/bigquery_project", "evaluate");
    const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    await dfapi.run(dbadapter, executionGraph).result();

    const tablesByName = keyBy(compiledGraph.tables, t => targetAsReadableString(t.target));
    const operationsByName = keyBy(compiledGraph.operations, t => targetAsReadableString(t.target));
    const assertionsByName = keyBy(compiledGraph.assertions, t => targetAsReadableString(t.target));

    const view =
      tablesByName[`${INTEGRATION_TEST_PROJECT}.df_integration_test_evaluate.example_view`];
    let evaluations = await dbadapter.evaluate(dataform.Table.create(view));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const materializedView =
      tablesByName[
        `${INTEGRATION_TEST_PROJECT}.df_integration_test_evaluate.example_materialized_view`
      ];
    evaluations = await dbadapter.evaluate(dataform.Table.create(materializedView));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const table =
      tablesByName[`${INTEGRATION_TEST_PROJECT}.df_integration_test_evaluate.example_table`];
    evaluations = await dbadapter.evaluate(dataform.Table.create(table));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const operation =
      operationsByName[
        `${INTEGRATION_TEST_PROJECT}.df_integration_test_evaluate.example_operation`
      ];
    evaluations = await dbadapter.evaluate(dataform.Operation.create(operation));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const assertion =
      assertionsByName[
        `${INTEGRATION_TEST_PROJECT}.df_integration_test_assertions_evaluate.example_assertion_pass`
      ];
    evaluations = await dbadapter.evaluate(dataform.Assertion.create(assertion));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const incremental =
      tablesByName[`${INTEGRATION_TEST_PROJECT}.df_integration_test_evaluate.example_incremental`];
    evaluations = await dbadapter.evaluate(dataform.Table.create(incremental));
    expect(evaluations.length).to.equal(2);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);
    expect(evaluations[1].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);
  });

  test("variable persistence validated correctly", async () => {
    const target = (name: string) => ({
      schema: "df_integration_test",
      name,
      database: INTEGRATION_TEST_PROJECT
    });

    let evaluations = await dbadapter.evaluate(
      dataform.Table.create({
        enumType: dataform.TableType.TABLE,
        preOps: ["declare var string; set var = 'val';"],
        query: "select var as col;",
        target: target("example_valid_variable")
      })
    );
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    evaluations = await dbadapter.evaluate(
      dataform.Table.create({
        enumType: dataform.TableType.TABLE,
        query: "select var as col;",
        target: target("example_invalid_variable")
      })
    );
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE);
  });

  test("invalid table fails validation and error parsed correctly", async () => {
    const evaluations = await dbadapter.evaluate(
      dataform.Table.create({
        enumType: dataform.TableType.TABLE,
        query: "selects\n1 as x",
        target: {
          name: "EXAMPLE_ILLEGAL_TABLE",
          database: "df_integration_test"
        }
      })
    );
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE);
    expect(
      dataform.QueryEvaluationError.ErrorLocation.create(evaluations[0].error.errorLocation)
    ).eql(dataform.QueryEvaluationError.ErrorLocation.create({ line: 1, column: 1 }));
  });
});
