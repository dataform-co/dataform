import { IDbAdapter } from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

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
      cachedStates = [];
    }
  }
  return { tables: tablesWithValues, cachedStates };
}
