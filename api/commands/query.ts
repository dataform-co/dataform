import * as path from "path";

import { CompileChildProcess } from "df/api/commands/compile";
import * as dbadapters from "df/api/dbadapters";
import { CancellablePromise } from "df/api/utils/cancellable_promise";
import { ErrorWithCause } from "df/common/errors/errors";
import { BigQueryAdapter } from "df/core/adapters/bigquery";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

export function run(
  dbadapter: dbadapters.IDbAdapter,
  query: string,
  options?: {
    compileConfig?: dataform.ICompileConfig;
    maxResults?: number;
  }
): CancellablePromise<any[]> {
  return new CancellablePromise(async (resolve, reject, onCancel) => {
    try {
      const compiledQuery = await compile(query, options && options.compileConfig);
      const results = await dbadapter.execute(compiledQuery, {
        onCancel,
        interactive: true,
        maxResults: options && options.maxResults
      });
      resolve(results.rows);
    } catch (e) {
      reject(e);
    }
  });
}

export async function evaluate(
  dbadapter: dbadapters.IDbAdapter,
  queryStringOrTable: string | dataform.ITable,
  compileConfig?: dataform.ICompileConfig,
  projectConfig?: dataform.IProjectConfig
): Promise<dataform.IQueryEvaluationResponse> {
  console.log("queryStringOrTable", queryStringOrTable);
  let queryString = "";
  if (typeof queryStringOrTable === "string") {
    queryString = queryStringOrTable;
  } else {
    try {
      queryString = new BigQueryAdapter(projectConfig, version)
        .publishTasks(
          queryStringOrTable,
          { useSingleQueryPerAction: projectConfig.useSingleQueryPerAction },
          {}
        )
        .build()[0].statement;
    } catch (e) {
      throw new ErrorWithCause("Error building table for evaluation.", e);
    }
  }
  console.log("queryString", queryString);
  const compiledQuery = await compile(queryString, compileConfig);
  return await dbadapter.evaluate(compiledQuery);
}

export async function compile(
  query: string,
  compileConfig?: dataform.ICompileConfig
): Promise<string> {
  // If there is no project directory, no need to compile the script.
  if (!compileConfig || !compileConfig.projectDir) {
    return Promise.resolve(query);
  }
  // Resolve the path in case it hasn't been resolved already.
  const projectDir = path.resolve(compileConfig.projectDir);

  query = query.replace(/\\/g, "\\\\").replace(/`/g, "\\`");

  return await CompileChildProcess.forkProcess().compile({
    ...compileConfig,
    projectDir,
    query,
    // For backwards compatibility with old versions of @dataform/core.
    returnOverride: `(function() {
      try {
        const ref = global.session.resolve.bind(global.session);
        const resolve = global.session.resolve.bind(global.session);
        const self = () => "";
        return \`${query}\`;
      } catch (e) {
        return e.message;
      }
    })()`
  });
}
