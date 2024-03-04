import { compile as compiler } from "df/core/compilers";
import { main } from "df/core/main";
import { Session } from "df/core/session";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

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

const supportedFeatures = [dataform.SupportedFeatures.ARRAY_BUFFER_IPC];

// Older versions of the CLI are not compatible with Core version ^3.0.0, and throw when this method
// is not available. Instead this more interpretable error message is thrown.
// Note: for future backwards compatability breaking changes, the exported "version" variable should
// be used instead.
function indexFileGenerator() {
  throw new Error("@dataform/cli ^3.0.0 required.");
}

// These exports constitute the public API of @dataform/core.
// They must also be listed in packages/@dataform/core/index.ts.
// Changes to these will break @dataform/cli, so take care!
export { compiler, indexFileGenerator, main, session, supportedFeatures, version };
