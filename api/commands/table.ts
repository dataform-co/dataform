import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

export async function list(
  credentials: Credentials,
  warehouse: string
): Promise<dataform.ITarget[]> {
  const dbadapter = dbadapters.create(credentials, warehouse);
  const tables = await dbadapter.tables();
  await dbadapter.close();
  return tables;
}

export async function get(
  credentials: Credentials,
  warehouse: string,
  target: dataform.ITarget
): Promise<dataform.ITableMetadata> {
  const dbadapter = dbadapters.create(credentials, warehouse);
  const table = await dbadapter.table(target);
  await dbadapter.close();
  return table;
}

export async function preview(
  credentials: Credentials,
  warehouse: string,
  target: dataform.ITarget,
  limitRows?: number
): Promise<any[]> {
  const dbadapter = dbadapters.create(credentials, warehouse);
  const rows = await dbadapter.preview(target, limitRows);
  await dbadapter.close();
  return rows;
}
