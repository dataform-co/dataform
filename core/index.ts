import * as adapters from "df/core/adapters";
import { compile as compiler, compileStandaloneSqlxQuery } from "df/core/compilers";
import { genIndex as indexFileGenerator } from "df/core/gen_index";
import { main } from "df/core/main";
import { Session } from "df/core/session";
import { version as dataformCoreVersion } from "df/core/version";

// Create static session object.
// This hack just enforces the singleton session object to
// be the same, regardless of the @dataform/core package that is running.
function globalSession() {
  if (!(global as any)._DF_SESSION) {
    (global as any)._DF_SESSION = new Session();
  }
  return (global as any)._DF_SESSION as Session;
}
const session = globalSession();

const supportedFeatures = {
  arrayBufferIPC: true
};

// These exports constitute the public API of @dataform/core.
// Changes to these will break @dataform/api, so take care!
export {
  adapters,
  dataformCoreVersion,
  indexFileGenerator,
  compiler,
  main,
  compileStandaloneSqlxQuery,
  session,
  supportedFeatures
};
