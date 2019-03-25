import * as protos from "@dataform/protos";
import { DbAdapter } from "../dbadapters";

export function state(
  compiledGraph: protos.ICompiledGraph,
  dbadapter: DbAdapter
): Promise<protos.IWarehouseState> {
  const tables: protos.ITableMetadata[] = [];

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
