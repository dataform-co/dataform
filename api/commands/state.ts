import { IDbAdapter } from "df/api/dbadapters";
import { dataform } from "df/protos";

export async function state(
  compiledGraph: dataform.ICompiledGraph,
  dbadapter: IDbAdapter
): Promise<dataform.IWarehouseState> {
  const allTables = await Promise.all(
    compiledGraph.tables.map(async t => dbadapter.table(t.target))
  );
  // filter out tables that don't exist
  const tablesWithValues = allTables.filter(table => {
    return !!table && !!table.type;
  });

  let cachedStates: dataform.IPersistedTableMetadata[] = null;

  if (compiledGraph.projectConfig.useRunCache) {
    try {
      cachedStates = await dbadapter.persistedStateMetadata();
    } catch (err) {
      // if the table doesn't exist or for some network error
      // cache state is not fetchable, then return empty array
      // which implies no caching will be done
      cachedStates = [];
    }
  }
  return { tables: tablesWithValues, cachedStates };
}
