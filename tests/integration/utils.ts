import { expect } from "chai";

import * as dfapi from "df/cli/api";
import * as dbadapters from "df/cli/api/dbadapters";
import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import { INTEGRATION_TEST_PROJECT } from "df/cli/index_test_base";
import { dataform } from "df/protos/ts";

export function keyBy<V>(values: V[], keyFn: (value: V) => string): { [key: string]: V } {
  return values.reduce((map, value) => {
    map[keyFn(value)] = value;
    return map;
  }, {} as { [key: string]: V });
}

export async function dropAllTables(
  tables: dataform.ITableMetadata[],
  executionSql: ExecutionSql,
  dbadapter: dbadapters.IDbAdapter
) {
  await Promise.all(
    tables.map(table => dbadapter.execute(executionSql.dropIfExists(table.target, table.type)))
  );
}

export async function getTableRows(
  target: dataform.ITarget,
  executionSql: ExecutionSql,
  dbadapter: dbadapters.IDbAdapter
) {
  return (await dbadapter.execute(`SELECT * FROM ${executionSql.resolveTarget(target)}`)).rows;
}

export async function compile(
  projectDir: string,
  schemaSuffixOverride: string,
  projectConfigOverrides?: dataform.IProjectConfig
) {
  const compiledGraph = await dfapi.compile({
    projectDir,
    projectConfigOverride: {
      defaultDatabase: INTEGRATION_TEST_PROJECT,
      schemaSuffix: schemaSuffixOverride
    }
  });

  expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);

  compiledGraph.projectConfig = {
    ...compiledGraph.projectConfig,
    ...projectConfigOverrides
  };
  return compiledGraph;
}

export async function cleanWarehouse(
  compiledGraph: dataform.CompiledGraph,
  dbadapter: dbadapters.IDbAdapter
) {
  await dropAllTables(
    (await dfapi.build(compiledGraph, {}, dbadapter)).warehouseState.tables,
    new ExecutionSql(compiledGraph.projectConfig, compiledGraph.dataformCoreVersion),
    dbadapter
  );
}
