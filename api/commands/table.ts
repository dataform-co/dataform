import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

export async function list(dbadapter: dbadapters.IDbAdapter): Promise<dataform.ITarget[]> {
  return await dbadapter.tables();
}

export async function get(
  dbadapter: dbadapters.IDbAdapter,
  target: dataform.ITarget
): Promise<dataform.ITableMetadata> {
  return await dbadapter.table(target);
}

export async function preview(
  dbadapter: dbadapters.IDbAdapter,
  target: dataform.ITarget,
  limitRows?: number
): Promise<any[]> {
  return await dbadapter.preview(target, limitRows);
}
