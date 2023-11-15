import * as compilers from "df/core/compilers";
import { genIndex } from "df/core/gen_index";
import { Session } from "df/core/session";
import { getWorkflowSettings } from "df/core/workflow_settings";

// These exports constitute the public API of @dataform/core.
// Changes to these will break @dataform/api, so take care!
export const indexFileGenerator = genIndex;
export const compiler = compilers.compile;
export { main } from "df/core/main";

// Create static session object.
// This hack just enforces the singleton session object to
// be the same, regardless of the @dataform/core package that is running.
function globalSession() {
  if (!(global as any)._DF_SESSION) {
    (global as any)._DF_SESSION = new Session();
  }
  return (global as any)._DF_SESSION as Session;
}
export const session = globalSession();

export { getWorkflowSettings };
