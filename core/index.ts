import * as protos from "@dataform/protos";
import { Session } from "./session";
import { Materialization, MaterializationContext, MContextable, MConfig } from "./materialization";
import { Operation, OContextable } from "./operation";
import { Assertion, AContextable } from "./assertion";

// Export only imports.

import * as adapters from "./adapters";
import * as utils from "./utils";
import * as compilers from "./compilers";
import * as tasks from "./tasks";

// Exports.

export {
  adapters,
  utils,
  compilers,
  tasks,
  Session,
  Materialization,
  MaterializationContext,
  MConfig,
  Operation,
  Assertion
};

// Install extensions for SQL files.

if (require.extensions) {
  require.extensions[".sql"] = function(module: any, file: string) {
    var oldCompile = module._compile;
    module._compile = function(code, file) {
      module._compile = oldCompile;
      var transformedCode = compilers.compile(code, file);
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

export const materialize = (name: string, queryOrConfig?: MContextable<string> | MConfig) =>
  session.materialize(name, queryOrConfig);
export const operate = (name: string, statement?: OContextable<string | string[]>) => session.operate(name, statement);
export const assert = (name: string, query?: AContextable<string>) => session.assert(name, query);
export const compile = () => session.compile();
export const init = (rootDir: string, projectConfig?: protos.IProjectConfig) => session.init(rootDir, projectConfig);

(global as any).session = session;
(global as any).materialize = materialize;
(global as any).operate = operate;
(global as any).assert = assert;
