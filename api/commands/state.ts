import * as protos from "@dataform/protos";
import { DbAdapter } from "../dbadapters";

export function state(compiledGraph: protos.ICompiledGraph, dbadapter: DbAdapter): Promise<protos.IWarehouseState> {
  const tables: protos.ITableState[] = [];

  return Promise.all(
    compiledGraph.tables.map(t =>
      dbadapter
        .table(t.target)
        .then(table => {
          if (table.type) {
            tables.push({ target: table.target, type: table.type });
          }
        })
        .catch(_ => {})
    )
  ).then(() => ({ tables: tables }));
}
