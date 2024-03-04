import * as dbadapters from "df/cli/api/dbadapters";
import { CancellablePromise } from "df/cli/api/utils/cancellable_promise";
import { dataform } from "df/protos/ts";

export function run(
  dbadapter: dbadapters.IDbAdapter,
  query: string,
  options?: {
    compileConfig?: dataform.ICompileConfig;
    rowLimit?: number;
    byteLimit?: number;
  }
): CancellablePromise<any[]> {
  return new CancellablePromise(async (resolve, reject, onCancel) => {
    try {
      const results = await dbadapter.execute(query, {
        onCancel,
        interactive: true,
        rowLimit: options?.rowLimit,
        byteLimit: options?.byteLimit
      });
      resolve(results.rows);
    } catch (e) {
      reject(e);
    }
  });
}

export async function evaluate(
  dbadapter: dbadapters.IDbAdapter,
  query: string
): Promise<dataform.IQueryEvaluation> {
  return (await dbadapter.evaluate(query))[0];
}
