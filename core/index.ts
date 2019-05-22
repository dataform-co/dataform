import { dataform } from "@dataform/protos";
import { AContextable, Assertion } from "./assertion";
import { OContextable, Operation } from "./operation";
import { Session } from "./session";
import { Table, TableContext, TConfig, TContextable } from "./table";

// Export only imports.

import * as adapters from "./adapters";
import * as compilers from "./compilers";
import { genIndex } from "./gen_index";
import * as tasks from "./tasks";
import * as utils from "./utils";

// Exports.

// TODO: Once all @dataform/core users *only* use the exported indexFileGenerator/compiler functions
// trim down exports to only include those (plus whatever might be called in the generated code returned
// by indexFileGenerator).

export const indexFileGenerator = genIndex;
export const compiler = compilers.compile;

export { adapters, utils, tasks, Session, Table, TableContext, TConfig, Operation, Assertion };

// Install extensions for SQL files.

if (require.extensions) {
  require.extensions[".sql"] = function(module: any, file: string) {
    const oldCompile = module._compile;
    module._compile = function(code: any, file: any) {
      module._compile = oldCompile;
      const transformedCode = compilers.compile(code, file);
      module._compile(transformedCode, file);
    };
    require.extensions[".js"](module, file);
  };
}

// Create static session object and bind global functions.

// TODO: Lerna causes issues here, as a package get's included via nested
// node_modules, this breaks the require cache and we end up with multiple
// @dataform/core packages being loaded and referenced by different packages
// during development. This hack just enforces the singleton session object to
// be the same, regardless of the @dataform/core package that is running.
const existingGlobalSession = (global as any)._DF_SESSION;
export const session = existingGlobalSession || new Session(process.cwd());
(global as any)._DF_SESSION = session;

export const publish = (name: string, queryOrConfig?: TContextable<string> | TConfig) =>
  session.publish(name, queryOrConfig);
export const materialize = publish;
export const operate = (name: string, statement?: OContextable<string | string[]>) =>
  session.operate(name, statement);
export const assert = (name: string, query?: AContextable<string>) => session.assert(name, query);
export const compile = () => session.compile();
export const init = (rootDir: string, projectConfig?: dataform.IProjectConfig) =>
  session.init(rootDir, projectConfig);

(global as any).session = session;
(global as any).materialize = publish;
(global as any).publish = publish;
(global as any).operate = operate;
(global as any).assert = assert;
