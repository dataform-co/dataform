import * as adapters from "@dataform/core/adapters";
import * as compilers from "@dataform/core/compilers";
import { genIndex } from "@dataform/core/gen_index";
import { Session } from "@dataform/core/session";

// These exports constitute the public API of @dataform/core.
// Changes to these will break @dataform/api, so take care!
export const indexFileGenerator = genIndex;
export const compiler = compilers.compile;
export { adapters };

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
// This hack just enforces the singleton session object to
// be the same, regardless of the @dataform/core package that is running.
function globalSession() {
  if (!(global as any)._DF_SESSION) {
    (global as any)._DF_SESSION = new Session(process.cwd());
  }
  return (global as any)._DF_SESSION as Session;
}
const session = globalSession();

(global as any).session = session;
(global as any).publish = session.publish.bind(session);
(global as any).operate = session.operate.bind(session);
(global as any).assert = session.assert.bind(session);
(global as any).test = session.test.bind(session);
