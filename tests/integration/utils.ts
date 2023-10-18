import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import { ExecutionSqlAdapter } from "df/api/dbadapters/execution_sql_adapter";
import { dataform } from "df/protos/ts";

export function keyBy<V>(values: V[], keyFn: (value: V) => string): { [key: string]: V } {
  return values.reduce((map, value) => {
    map[keyFn(value)] = value;
    return map;
  }, {} as { [key: string]: V });
}

export async function dropAllTables(
  tables: dataform.ITableMetadata[],
  executionSqlAdapter: ExecutionSqlAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  await Promise.all(
    tables.map(table =>
      dbadapter.execute(executionSqlAdapter.dropIfExists(table.target, table.type))
    )
  );
}

export async function getTableRows(
  target: dataform.ITarget,
  executionSqlAdapter: ExecutionSqlAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  return (await dbadapter.execute(`SELECT * FROM ${executionSqlAdapter.resolveTarget(target)}`))
    .rows;
}

export async function compile(
  projectDir: string,
  schemaSuffixOverride: string,
  projectConfigOverrides?: dataform.IProjectConfig
) {
  const compiledGraph = await dfapi.compile({
    projectDir,
    projectConfigOverride: { schemaSuffix: schemaSuffixOverride }
  });

  expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);

  compiledGraph.projectConfig = {
    ...compiledGraph.projectConfig,
    ...projectConfigOverrides
  };
  return compiledGraph;
}
