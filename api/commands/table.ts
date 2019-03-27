import { dataform } from "@dataform/protos";
import * as dbadapters from "df/api/dbadapters";
import * as utils from "df/api/utils";

export function list(credentials: utils.Credentials): Promise<dataform.ITarget[]> {
  return dbadapters.create(credentials).tables();
}

export function get(
  credentials: utils.Credentials,
  target: dataform.ITarget
): Promise<dataform.ITableMetadata> {
  return dbadapters.create(credentials).table(target);
}
