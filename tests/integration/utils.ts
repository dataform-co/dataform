import * as dbadapters from "@dataform/api/dbadapters";
import * as adapters from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";

export function keyBy<V>(values: V[], keyFn: (value: V) => string): { [key: string]: V } {
  return values.reduce(
    (map, value) => {
      map[keyFn(value)] = value;
      return map;
    },
    {} as { [key: string]: V }
  );
}

export async function dropAllTables(
  compiledGraph: dataform.ICompiledGraph,
  adapter: adapters.IAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  await Promise.all(
    [].concat(
      compiledGraph.tables.map(table =>
        dbadapter.execute(adapter.dropIfExists(table.target, adapter.baseTableType(table.type)))
      ),
      compiledGraph.assertions.map(assertion =>
        dbadapter.execute(adapter.dropIfExists(assertion.target, "view"))
      )
    )
  );
}

export async function getTableRows(
  target: dataform.ITarget,
  adapter: adapters.IAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  return dbadapter.execute(`select * from ${adapter.resolveTarget(target)}`);
}
