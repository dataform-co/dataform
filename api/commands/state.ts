import { IDbAdapter } from "df/api/dbadapters";
import { dataform } from "df/protos/ts";

export async function state(
  dbadapter: IDbAdapter,
  targets: dataform.ITarget[],
  fetchPersistedMetadata: boolean
): Promise<dataform.IWarehouseState> {
  const allTables = await Promise.all(targets.map(async target => dbadapter.table(target)));

  // Filter out datasets that don't exist.
  const tablesWithValues = allTables.filter(table => {
    return !!table && !!table.type;
  });

  let cachedStates: dataform.IPersistedTableMetadata[] = null;

  if (fetchPersistedMetadata) {
    try {
      cachedStates = await dbadapter.persistedStateMetadata();
    } catch (err) {
      // If the table doesn't exist or for some network error
      // cache state is not fetchable, then return empty array
      // which implies no caching will be done.
      cachedStates = [];
    }
  }
  return { tables: tablesWithValues, cachedStates };
}
