import { CompileChildProcess } from "@dataform/api/commands/compile";
import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { CancellablePromise } from "@dataform/api/utils/cancellable_promise";
import { fork } from "child_process";
import * as path from "path";

interface IOptions {
  projectDir?: string;
}

export function run(
  credentials: Credentials,
  warehouse: string,
  query: string,
  options?: IOptions
): CancellablePromise<any[]> {
  return new CancellablePromise(async (_resolve, _reject, onCancel) => {
    try {
      const compiledQuery = await compile(query, options);
      const results = await dbadapters
        .create(credentials, warehouse)
        .execute(compiledQuery, { onCancel, interactive: true });
      _resolve(results);
    } catch (e) {
      _reject(e);
    }
  });
}

export function evaluate(
  credentials: Credentials,
  warehouse: string,
  query: string,
  options?: IOptions
): Promise<void> {
  return compile(query, options).then(compiledQuery =>
    dbadapters.create(credentials, warehouse).evaluate(compiledQuery)
  );
}

export async function compile(query: string, options?: IOptions): Promise<string> {
  // If there is no project directory, no need to compile the script.
  if (!options || !options.projectDir) {
    return Promise.resolve(query);
  }
  // Resolve the path in case it hasn't been resolved already.
  const projectDir = path.resolve(options.projectDir);

  return await CompileChildProcess.forkProcess().compile({
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
