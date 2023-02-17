import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import * as adapters from "df/core/adapters";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export function keyBy<V>(values: V[], keyFn: (value: V) => string): { [key: string]: V } {
  return values.reduce((map, value) => {
    map[keyFn(value)] = value;
    return map;
  }, {} as { [key: string]: V });
}

export async function dropAllTables(
  tables: execution.TableMetadata[],
  adapter: adapters.IAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  await Promise.all(
    tables.map(table => dbadapter.execute(adapter.dropIfExists(table.target, table.type)))
  );
}

export async function getTableRows(
  target: core.Target,
  adapter: adapters.IAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  return (await dbadapter.execute(`SELECT * FROM ${adapter.resolveTarget(target)}`)).rows;
}

export async function compile(
  projectDir: string,
  schemaSuffixOverride: string,
  projectConfigOverrides?: core.ProjectConfig
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
