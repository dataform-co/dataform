import * as protos from "@dataform/protos";

export function state(compiledGraph: protos.ICompiledGraph): Promise<protos.IWarehouseState> {
  var tables: protos.ITableState[] = [];

  return Promise.all(
    compiledGraph.materializations.map(m =>
      this.dbadapter
        .table(m.target)
        .then(table => {
          tables.push({ target: table.target, type: table.type });
        })
        .catch(_ => {})
    )
  ).then(() => ({ tables: tables }));
}
