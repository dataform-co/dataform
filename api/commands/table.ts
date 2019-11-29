import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

export async function list(
  credentials: Credentials,
  warehouse: string
): Promise<dataform.ITarget[]> {
  const dbadapter = dbadapters.create(credentials, warehouse);
  try {
    return await dbadapter.tables();
  } finally {
    await dbadapter.close();
  }
}

export async function get(
  credentials: Credentials,
  warehouse: string,
  target: dataform.ITarget
): Promise<dataform.ITableMetadata> {
  const dbadapter = dbadapters.create(credentials, warehouse);
  try {
    return await dbadapter.table(target);
  } finally {
    await dbadapter.close();
  }
}

export async function preview(
  credentials: Credentials,
  warehouse: string,
  target: dataform.ITarget,
  limitRows?: number
): Promise<any[]> {
  const dbadapter = dbadapters.create(credentials, warehouse);
  try {
    return await dbadapter.preview(target, limitRows);
  } finally {
    await dbadapter.close();
  }
}
