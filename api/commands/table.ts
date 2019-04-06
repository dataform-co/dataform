import * as dbadapters from "@dataform/api/dbadapters";
import * as utils from "@dataform/api/utils";
import { dataform } from "@dataform/protos";

export function list(
  credentials: utils.Credentials,
  warehouse: string
): Promise<dataform.ITarget[]> {
  return dbadapters.create(credentials, warehouse).tables();
}

export function get(
  credentials: utils.Credentials,
  warehouse: string,
  target: dataform.ITarget
): Promise<dataform.ITableMetadata> {
  return dbadapters.create(credentials, warehouse).table(target);
}
