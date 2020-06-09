import { IDbAdapter } from "df/api/dbadapters";
import { dataform } from "df/protos/ts";

export async function state(
  compiledGraph: dataform.ICompiledGraph,
  dbadapter: IDbAdapter
): Promise<dataform.IWarehouseState> {
  const allTables = await Promise.all([
    ...compiledGraph.tables.map(async table => dbadapter.table(table.target)),
    ...compiledGraph.operations
      .filter(operation => operation.hasOutput)
      .map(async operation => dbadapter.table(operation.target)),
    ...compiledGraph.assertions.map(async assertion => dbadapter.table(assertion.target))
  ]);

  // Filter out datasets that don't exist.
  const tablesWithValues = allTables.filter(table => {
    return !!table && !!table.type;
  });

  let cachedStates: dataform.IPersistedTableMetadata[] = null;

  if (compiledGraph.projectConfig.useRunCache) {
    try {
      cachedStates = await dbadapter.persistedStateMetadata();
    } catch (err) {
      // If the table doesn't exist or for some network error
      // cache state is not fetchable, then return empty array
      // which implies no caching will be done.
      cachedStates = [];
    }
  }
  return { tables: tablesWithValues, cachedStates };
}
