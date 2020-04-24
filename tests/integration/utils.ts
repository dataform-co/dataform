import { query } from "@dataform/api";
import { Credentials } from "@dataform/api/commands/credentials";
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
  tables: dataform.ITableMetadata[],
  adapter: adapters.IAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  await Promise.all(
    tables.map(table =>
      dbadapter.execute(adapter.dropIfExists(table.target, adapter.baseTableType(table.type)))
    )
  );
}

export async function getTableRows(
  target: dataform.ITarget,
  adapter: adapters.IAdapter,
  dbadapter: dbadapters.IDbAdapter
) {
  return (await dbadapter.execute(`SELECT * FROM ${adapter.resolveTarget(target)}`)).rows;
}
