import * as adapters from "df/core/adapters";
import * as compilers from "df/core/compilers";
import { genIndex } from "df/core/gen_index";
import { Session } from "df/core/session";

// These exports constitute the public API of @dataform/core.
// Changes to these will break @dataform/api, so take care!
export const indexFileGenerator = genIndex;
export const compiler = compilers.compile;
export { adapters };

// Create static session object.
// This hack just enforces the singleton session object to
// be the same, regardless of the @dataform/core package that is running.
function globalSession() {
  if (!(global as any)._DF_SESSION) {
    (global as any)._DF_SESSION = new Session(process.cwd());
  }
  return (global as any)._DF_SESSION as Session;
}
export const session = globalSession();
