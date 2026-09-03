import { expect } from "chai";
import { randomBytes } from "crypto";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { compile, keyBy } from "df/tests/integration/utils";

const PROJECT = "dataform-open-source";
const GRAPH_NAME = "LibraryGraph";

function makeSuffix() {
  return (
    process.env.GITHUB_RUN_ID ??
    process.env.BUILD_ID ??
    randomBytes(4).toString("hex")
  );
}

async function dropDataset(dbadapter: BigQueryDbAdapter, dataset: string) {
  await dbadapter.execute(
    `drop schema if exists \`${PROJECT}.${dataset}\` cascade`
  );
}

suite("@dataform/integration/property_graph", { parallel: true }, ({ before, after }) => {
  const credentials = dfapi.credentials.read("test_credentials/bigquery.json");
  const schemaSuffix = `e2e_${makeSuffix()}`;
  const dataset = `df_integration_test_pg_${schemaSuffix}`;
  const graphTarget = `${dataset}.${GRAPH_NAME}`;
  let dbadapter: BigQueryDbAdapter;

  before("create adapter", async () => {
    dbadapter = new BigQueryDbAdapter(credentials);
  });

  after("drop dataset", async () => {
    await dropDataset(dbadapter, dataset);
  });

  test("creates property graph end-to-end", { timeout: 120000 }, async () => {
    const compiledGraph = await compile(
      "tests/integration/property_graph_project",
      schemaSuffix
    );

    await dropDataset(dbadapter, dataset);

    const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    const executedGraph = await dfapi.run(dbadapter, executionGraph).result();

    const actionMap = keyBy(executedGraph.actions, v => targetAsReadableString(v.target));
    expect(Object.keys(actionMap)).to.have.lengthOf(4);
    for (const [name, action] of Object.entries(actionMap)) {
      expect(action.status).equals(
        dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        `${name}: ${JSON.stringify(action, null, 2)}`
      );
    }

    expect(actionMap).to.have.property(graphTarget);

    const rows = (await dbadapter.execute(
      `select property_graph_catalog, property_graph_schema,
              property_graph_name, ddl
       from \`${PROJECT}.${dataset}\`.INFORMATION_SCHEMA.PROPERTY_GRAPHS`
    )).rows;
    expect(rows).to.have.lengthOf(1);
    const [row] = rows;
    expect(row.property_graph_catalog).equals(PROJECT);
    expect(row.property_graph_schema).equals(dataset);
    expect(row.property_graph_name).equals(GRAPH_NAME);
    for (const needle of [
      "NODE TABLES", "EDGE TABLES",
      "Author", "Book", "Wrote",
      "authors", "books", "wrote"
    ]) {
      expect(row.ddl).to.contain(needle);
    }
  });
});
