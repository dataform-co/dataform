import { expect } from "chai";
import { randomBytes } from "crypto";

import * as dfapi from "df/cli/api";
import * as dbadapters from "df/cli/api/dbadapters";
import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import { INTEGRATION_TEST_PROJECT } from "df/cli/index_test_base";
import { dataform } from "df/protos/ts";

export function keyBy<V>(values: V[], keyFn: (value: V) => string): { [key: string]: V } {
  return values.reduce(
    (map, value) => {
      map[keyFn(value)] = value;
      return map;
    },
    {} as { [key: string]: V },
  );
}

// Suffix for resources (e.g. datasets) that one run of a test creates, so that concurrent runs never
// share them. It starts with the CI run ID if there is one, so that a leftover resource can be traced
// back to its run, and ends with random bytes, so that it's unique even within one CI run. Other
// characters than letters, digits and underscores (e.g. the dashes in a Cloud Build ID) are replaced,
// because BigQuery doesn't allow them in dataset names.
export function makeSuffix() {
  const runId = process.env.GITHUB_RUN_ID ?? process.env.BUILD_ID;
  const random = randomBytes(4).toString("hex");
  return (runId ? `${runId}_${random}` : random).replace(/[^A-Za-z0-9_]/g, "_");
}

export async function dropDataset(dbadapter: dbadapters.IDbAdapter, dataset: string) {
  await dbadapter.execute(
    `drop schema if exists \`${INTEGRATION_TEST_PROJECT}.${dataset}\` cascade`,
  );
}

export async function dropAllTables(
  tables: dataform.ITableMetadata[],
  executionSql: ExecutionSql,
  dbadapter: dbadapters.IDbAdapter,
) {
  await Promise.all(
    tables.map((table) => dbadapter.execute(executionSql.dropIfExists(table.target, table.type))),
  );
}

export async function getTableRows(
  target: dataform.ITarget,
  executionSql: ExecutionSql,
  dbadapter: dbadapters.IDbAdapter,
) {
  return (await dbadapter.execute(`SELECT * FROM ${executionSql.resolveTarget(target)}`)).rows;
}

export async function compile(
  projectDir: string,
  schemaSuffixOverride: string,
  projectConfigOverrides?: dataform.IProjectConfig,
) {
  const compiledGraph = await dfapi.compile({
    projectDir,
    projectConfigOverride: {
      defaultDatabase: INTEGRATION_TEST_PROJECT,
      schemaSuffix: schemaSuffixOverride,
    },
  });

  expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);

  compiledGraph.projectConfig = {
    ...compiledGraph.projectConfig,
    ...projectConfigOverrides,
  };
  return compiledGraph;
}

export async function cleanWarehouse(
  compiledGraph: dataform.CompiledGraph,
  dbadapter: dbadapters.IDbAdapter,
) {
  await dropAllTables(
    (await dfapi.build(compiledGraph, {}, dbadapter)).warehouseState.tables,
    new ExecutionSql(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion),
    dbadapter,
  );
}
