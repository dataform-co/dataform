import { expect } from "chai";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import {
  CREDENTIALS_PATH,
  INTEGRATION_TEST_LOCATION,
  INTEGRATION_TEST_PROJECT,
} from "df/cli/index_test_base";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { test } from "df/testing";
import { dropDatasets, dropStaleDatasets, makeUniqueSuffix } from "df/testing/bigquery";
import { integrationSuite } from "df/testing/credentials";
import { compile, datasetsOf, keyBy } from "df/tests/integration/utils";

const EVALUATE_SCHEMA_SUFFIX = "evaluate";
const EVALUATE_DATASET = `df_integration_test_${EVALUATE_SCHEMA_SUFFIX}`;
const EVALUATE_ASSERTIONS_DATASET = `df_integration_test_assertions_${EVALUATE_SCHEMA_SUFFIX}`;

integrationSuite("@dataform/integration/evaluate", ({ before, after }) => {
  const credentials = dfapi.credentials.read(CREDENTIALS_PATH);
  const dbadapter = new BigQueryDbAdapter(credentials);
  const uniqueSuffix = makeUniqueSuffix();

  // The datasets created by the tests, without the unique suffix.
  let datasets: string[];

  before("drop stale datasets", async () => {
    datasets = datasetsOf(
      await compile("tests/integration/bigquery_project", EVALUATE_SCHEMA_SUFFIX),
    );
    await dropStaleDatasets(dbadapter, {
      project: INTEGRATION_TEST_PROJECT,
      location: INTEGRATION_TEST_LOCATION,
      prefixes: datasets,
    });
  });

  after("drop datasets", async () => {
    await dropDatasets(
      dbadapter,
      INTEGRATION_TEST_PROJECT,
      datasets.map((dataset) => `${dataset}_${uniqueSuffix}`),
    );
  });

  test("evaluate from valid compiled graph as valid", async () => {
    // Create and run the project.
    const compiledGraph = await compile(
      "tests/integration/bigquery_project",
      `${EVALUATE_SCHEMA_SUFFIX}_${uniqueSuffix}`,
    );
    const dataset = `${EVALUATE_DATASET}_${uniqueSuffix}`;
    const assertionsDataset = `${EVALUATE_ASSERTIONS_DATASET}_${uniqueSuffix}`;
    const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    await dfapi.run(dbadapter, executionGraph).result();

    const tablesByName = keyBy(compiledGraph.tables, (t) => targetAsReadableString(t.target));
    const operationsByName = keyBy(compiledGraph.operations, (t) =>
      targetAsReadableString(t.target),
    );
    const assertionsByName = keyBy(compiledGraph.assertions, (t) =>
      targetAsReadableString(t.target),
    );

    const view = tablesByName[`${INTEGRATION_TEST_PROJECT}.${dataset}.example_view`];
    let evaluations = await dbadapter.evaluate(dataform.Table.create(view));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const materializedView =
      tablesByName[`${INTEGRATION_TEST_PROJECT}.${dataset}.example_materialized_view`];
    evaluations = await dbadapter.evaluate(dataform.Table.create(materializedView));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const table = tablesByName[`${INTEGRATION_TEST_PROJECT}.${dataset}.example_table`];
    evaluations = await dbadapter.evaluate(dataform.Table.create(table));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const operation = operationsByName[`${INTEGRATION_TEST_PROJECT}.${dataset}.example_operation`];
    evaluations = await dbadapter.evaluate(dataform.Operation.create(operation));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const assertion =
      assertionsByName[`${INTEGRATION_TEST_PROJECT}.${assertionsDataset}.example_assertion_pass`];
    evaluations = await dbadapter.evaluate(dataform.Assertion.create(assertion));
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    const incremental = tablesByName[`${INTEGRATION_TEST_PROJECT}.${dataset}.example_incremental`];
    evaluations = await dbadapter.evaluate(dataform.Table.create(incremental));
    expect(evaluations.length).to.equal(2);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);
    expect(evaluations[1].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);
  });

  test("variable persistence validated correctly", async () => {
    const target = (name: string) => ({
      schema: "df_integration_test",
      name,
      database: INTEGRATION_TEST_PROJECT,
    });

    let evaluations = await dbadapter.evaluate(
      dataform.Table.create({
        enumType: dataform.TableType.TABLE,
        preOps: ["declare var string; set var = 'val';"],
        query: "select var as col;",
        target: target("example_valid_variable"),
      }),
    );
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS);

    evaluations = await dbadapter.evaluate(
      dataform.Table.create({
        enumType: dataform.TableType.TABLE,
        query: "select var as col;",
        target: target("example_invalid_variable"),
      }),
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
          database: "df_integration_test",
        },
      }),
    );
    expect(evaluations.length).to.equal(1);
    expect(evaluations[0].status).to.equal(dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE);
    expect(
      dataform.QueryEvaluationError.ErrorLocation.create(evaluations[0].error.errorLocation),
    ).eql(dataform.QueryEvaluationError.ErrorLocation.create({ line: 1, column: 1 }));
  });
});
