import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

export function list(credentials: Credentials, warehouse: string): Promise<dataform.ITarget[]> {
  return dbadapters.create(credentials, warehouse).tables();
}

export function get(
  credentials: Credentials,
  warehouse: string,
  target: dataform.ITarget
): Promise<dataform.ITableMetadata> {
  return dbadapters.create(credentials, warehouse).table(target);
}

export function preview(
  credentials: Credentials,
  warehouse: string,
  target: dataform.ITarget
): Promise<any[]> {
  return dbadapters.create(credentials, warehouse).preview(target);
}
