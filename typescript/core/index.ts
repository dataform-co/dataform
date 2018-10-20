import * as protos from "@dataform/protos";
import { Dataform } from "./dataform";
import { Materialization, MContextable } from "./materialization";
import { Operation, OContextable } from "./operation";
import { Assertion, AContextable } from "./assertion";

// Export only imports.

import * as adapters from "./adapters";
import * as utils from "./utils";
import * as parser from "./parser";

// Exports.

export {
  adapters,
  utils,
  parser,
  Dataform,
  Materialization,
  Operation,
  Assertion
};

// Install extensions for SQL files.

if (require.extensions) {
  require.extensions[".sql"] = function(module: any, file: string) {
    var oldCompile = module._compile;
    module._compile = function(code, file) {
      module._compile = oldCompile;
      var transformedCode;
      if (file.endsWith(".test.sql")) {
        transformedCode = utils.compileAssertionSql(code, file);
      } else if (file.endsWith(".ops.sql")) {
        transformedCode = utils.compileOperationSql(code, file);
      } else {
        transformedCode = utils.compileMaterializationSql(code, file);
      }
      module._compile(transformedCode, file);
    };
    require.extensions[".js"](module, file);
  };
}

// Create static singleton object and bind global functions.

const singleton = new Dataform();

export const materialize = (name: string, query?: MContextable<string>) =>
  singleton.materialize(name, query);
export const operate = (
  name: string,
  statement?: OContextable<string | string[]>
) => singleton.operate(name, statement);
export const assert = (name: string, query?: AContextable<string | string[]>) =>
  singleton.assert(name, query);
export const compile = () => singleton.compile();
export const init = (projectConfig?: protos.IProjectConfig) =>
  singleton.init(projectConfig);

(global as any).materialize = materialize;
(global as any).operate = operate;
(global as any).assert = assert;
