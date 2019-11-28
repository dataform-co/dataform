import { IDbAdapter } from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

export async function state(
  compiledGraph: dataform.ICompiledGraph,
  dbadapter: IDbAdapter
): Promise<dataform.IWarehouseState> {
  const allTables = await Promise.all(
    compiledGraph.tables
      .map(async t => dbadapter.table(t.target))
      // Skip tables that don't exist.
      .filter(async table => !!(await table).type)
  );

  return { tables: allTables };
}
