import { IDbAdapter } from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

export async function state(
  compiledGraph: dataform.ICompiledGraph,
  dbadapter: IDbAdapter
): Promise<dataform.IWarehouseState> {
  const allTables = await Promise.all(
    compiledGraph.tables
      .map(async t => {
        const table = await dbadapter.table(t.target);
        return table.type ? table : null;
      })
      .filter(async table => !!(await table))
  );

  return { tables: allTables };
}
