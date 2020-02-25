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

  const cachedStates = await dbadapter.persistedStateMetadata(compiledGraph);

  console.log(compiledGraph.projectConfig, cachedStates);

  return { tables: tablesWithValues, cachedStates };
}
