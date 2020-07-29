import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import * as adapters from "df/core/adapters";
import { dataform } from "df/protos/ts";

export function keyBy<V>(values: V[], keyFn: (value: V) => string): { [key: string]: V } {
  return values.reduce((map, value) => {
    map[keyFn(value)] = value;
    return map;
  }, {} as { [key: string]: V });
}

export async function dropAllTables(
  tables: dataform.ITableMetadata[],
  adapter: adapters.IAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  await Promise.all(
    tables.map(table => dbadapter.execute(adapter.dropIfExists(table.target, table.type)))
  );
}

export async function getTableRows(
  target: dataform.ITarget,
  adapter: adapters.IAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  return (await dbadapter.execute(`SELECT * FROM ${adapter.resolveTarget(target)}`)).rows;
}

export async function compile(
  projectDir: string,
  schemaSuffixOverride: string,
  projectConfigOverrides?: dataform.IProjectConfig
) {
  const compiledGraph = await dfapi.compile({
    projectDir,
    schemaSuffixOverride
  });

  expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);

  compiledGraph.projectConfig = {
    ...compiledGraph.projectConfig,
    ...projectConfigOverrides
  };
  return compiledGraph;
}
