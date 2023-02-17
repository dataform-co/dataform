import { IDbAdapter } from "df/api/dbadapters";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export async function state(
  dbadapter: IDbAdapter,
  targets: core.Target[]
): Promise<dataform.WarehouseState> {
  const allTables = await Promise.all(targets.map(async target => dbadapter.table(target)));

  // Filter out datasets that don't exist.
  const tablesWithValues = allTables.filter(table => {
    return !!table && !!table.type;
  });
  return { tables: tablesWithValues };
}
