import * as dbadapters from "df/api/dbadapters";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export async function list(dbadapter: dbadapters.IDbAdapter): Promise<core.Target[]> {
  return await dbadapter.tables();
}

export async function get(
  dbadapter: dbadapters.IDbAdapter,
  target: core.Target
): Promise<execution.TableMetadata> {
  return await dbadapter.table(target);
}

export async function preview(
  dbadapter: dbadapters.IDbAdapter,
  target: core.Target,
  limitRows?: number
): Promise<any[]> {
  return await dbadapter.preview(target, limitRows);
}
