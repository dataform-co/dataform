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
import { integrationSuite } from "df/testing/credentials";
import { dropDatasets, dropStaleDatasets, makeUniqueSuffix } from "df/testing/bigquery";
import { compile, keyBy } from "df/tests/integration/utils";

const GRAPH_NAME = "LibraryGraph";

// The test compiles property_graph_project with this schema suffix, which dataform appends to the
// project's dataset. The unique suffix is appended to the names below.
const SCHEMA_SUFFIX = "e2e";
const DATASET = `df_integration_test_pg_${SCHEMA_SUFFIX}`;

integrationSuite(
  "@dataform/integration/property_graph",
  { parallel: true },
  ({ before, after }) => {
    const credentials = dfapi.credentials.read(CREDENTIALS_PATH);
    const uniqueSuffix = makeUniqueSuffix();
    const dataset = `df_integration_test_pg_${schemaSuffix}`;
    const graphTarget = `${INTEGRATION_TEST_PROJECT}.${dataset}.${GRAPH_NAME}`;
    let dbadapter: BigQueryDbAdapter;

    before("create adapter", async () => {
      dbadapter = new BigQueryDbAdapter(credentials);
    });

    before("drop stale datasets", async () => {
      await dropStaleDatasets(dbadapter, {
        project: INTEGRATION_TEST_PROJECT,
        location: INTEGRATION_TEST_LOCATION,
        prefixes: [DATASET],
      });
    });

    after("drop datasets", async () => {
      await dropDatasets(dbadapter, INTEGRATION_TEST_PROJECT, [dataset]);
    });

    test("creates property graph end-to-end", { timeout: 120000 }, async () => {
      const compiledGraph = await compile(
        "tests/integration/property_graph_project",
        `${SCHEMA_SUFFIX}_${uniqueSuffix}`,
      );

      const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
      const executedGraph = await dfapi.run(dbadapter, executionGraph).result();

      const actionMap = keyBy(executedGraph.actions, (v) => targetAsReadableString(v.target));
      expect(Object.keys(actionMap)).to.have.lengthOf(4);
      for (const [name, action] of Object.entries(actionMap)) {
        expect(action.status).equals(
          dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
          `${name}: ${JSON.stringify(action, null, 2)}`,
        );
      }

      expect(actionMap).to.have.property(graphTarget);

      const rows = (
        await dbadapter.execute(
          `select property_graph_catalog, property_graph_schema,
              property_graph_name, ddl
       from \`${INTEGRATION_TEST_PROJECT}.${dataset}\`.INFORMATION_SCHEMA.PROPERTY_GRAPHS`,
        )
      ).rows;
      expect(rows).to.have.lengthOf(1);
      const [row] = rows;
      expect(row.property_graph_catalog).equals(INTEGRATION_TEST_PROJECT);
      expect(row.property_graph_schema).equals(dataset);
      expect(row.property_graph_name).equals(GRAPH_NAME);
      for (const needle of [
        "NODE TABLES",
        "EDGE TABLES",
        "Author",
        "Book",
        "Wrote",
        "authors",
        "books",
        "wrote",
      ]) {
        expect(row.ddl).to.contain(needle);
      }
    });
  },
);
