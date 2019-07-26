import { dataform } from "@dataform/protos";
import { IDbAdapter } from "../dbadapters";

export function state(
  compiledGraph: dataform.ICompiledGraph,
  dbadapter: IDbAdapter
): Promise<dataform.IWarehouseState> {
  const tables: dataform.ITableMetadata[] = [];

  return Promise.all(
    compiledGraph.tables.map(t =>
      dbadapter
        .table(t.target)
        .then(table => {
          // Skip tables that don't exist.
          if (table.type) {
            tables.push(table);
          }
        })
        .catch(_ => {})
    )
  ).then(() => ({ tables }));
}
