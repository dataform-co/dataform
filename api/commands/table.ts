import * as protos from "@dataform/protos";
import * as dbadapters from "../dbadapters";

export function list(profile: protos.IProfile): Promise<protos.ITarget[]> {
  return dbadapters.create(profile).tables();
}

export function get(
  profile: protos.IProfile,
  target: protos.ITarget
): Promise<protos.ITableMetadata> {
  return dbadapters.create(profile).table(target);
}
