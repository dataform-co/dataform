import { IDbAdapter } from "df/cli/api/dbadapters";
import { dataform } from "df/protos/ts";

export async function state(
  dbadapter: IDbAdapter,
  targets: dataform.ITarget[]
): Promise<dataform.IWarehouseState> {
  const allTables = await Promise.all(targets.map(async target => dbadapter.table(target)));

  // Filter out datasets that don't exist.
  const tablesWithValues = allTables.filter(table => {
    return !!table && !!table.type;
  });
  return { tables: tablesWithValues };
}
