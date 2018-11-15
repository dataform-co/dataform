import * as protos from "@dataform/protos";
import { DbAdapter } from "../dbadapters";

export function state(compiledGraph: protos.ICompiledGraph, dbadapter: DbAdapter): Promise<protos.IWarehouseState> {
  var tables: protos.ITableState[] = [];

  return Promise.all(
    compiledGraph.materializations.map(m =>
      dbadapter
        .table(m.target)
        .then(table => {
          tables.push({ target: table.target, type: table.type });
        })
        .catch(_ => {})
    )
  ).then(() => ({ tables: tables }));
}
