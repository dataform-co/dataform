import * as dbadapters from "df/api/dbadapters";
import { CancellablePromise } from "df/api/utils/cancellable_promise";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export function run(
  dbadapter: dbadapters.IDbAdapter,
  query: string,
  options?: {
    compileConfig?: dataform.CompileConfig;
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
): Promise<dataform.QueryEvaluation> {
  return (await dbadapter.evaluate(query))[0];
}
