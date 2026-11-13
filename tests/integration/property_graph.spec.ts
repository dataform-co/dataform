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
    const uniqueDataset = `${DATASET}_${uniqueSuffix}`;
    const graphTarget = `${INTEGRATION_TEST_PROJECT}.${uniqueDataset}.${GRAPH_NAME}`;
    let dbadapter: BigQueryDbAdapter;

    before("create adapter", async () => {
      dbadapter = new BigQueryDbAdapter(credentials);
    });

    // The datasets created by the test, without the unique suffix.
    let datasets: string[];

    before("drop stale datasets", async () => {
      datasets = datasetsOf(
        await compile("tests/integration/property_graph_project", SCHEMA_SUFFIX),
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

    test("creates property graph end-to-end", { timeout: 120000 }, async () => {
      const compiledGraph = await compile(
        "tests/integration/property_graph_project",
        `${SCHEMA_SUFFIX}_${uniqueSuffix}`,
      );

      const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
      const executedGraph = await dfapi.run(dbadapter, executionGraph).result();

      const actionMap = keyBy(executedGraph.actions, (v) => targetAsReadableString(v.target));
      expect(Object.keys(actionMap)).to.have.lengthOf(5);
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
       from \`${INTEGRATION_TEST_PROJECT}.${uniqueDataset}\`.INFORMATION_SCHEMA.PROPERTY_GRAPHS`,
        )
      ).rows;
      expect(rows).to.have.lengthOf(1);
      const [row] = rows;
      expect(row.property_graph_catalog).equals(INTEGRATION_TEST_PROJECT);
      expect(row.property_graph_schema).equals(uniqueDataset);
      expect(row.property_graph_name).equals(GRAPH_NAME);
      for (const needle of [
        "NODE TABLES",
        "EDGE TABLES",
        "Author",
        "Book",
        "Authorship",
        "Wrote",
        "WroteBook",
        "authors",
        "books",
        "wrote",
        "UPPER(name) AS upper_name",
        "address.country AS country",
        "`group`",
        "DEFAULT LABEL OPTIONS",
        "extensions=",
        '"instructions"',
        '"examples"',
        "NO PROPERTIES",
        "MEASURE(COUNT(id)) AS book_count",
        "MEASURE(SUM(pages)) AS total_pages",
        "MEASURE(SAFE_DIVIDE(AGG(total_pages), NULLIF(AGG(book_count), 0))) AS avg_pages",
      ]) {
        expect(row.ddl).to.contain(needle);
      }

      const metricRows = (
        await dbadapter.execute(
          `select book_count, total_pages, avg_pages
       from \`${INTEGRATION_TEST_PROJECT}.${uniqueDataset}.book_metrics\``,
        )
      ).rows;
      expect(metricRows).to.have.lengthOf(1);
      expect(Number(metricRows[0].book_count)).equals(3);
      expect(Number(metricRows[0].total_pages)).equals(600);
      expect(Number(metricRows[0].avg_pages)).equals(200);
    });
  },
);
