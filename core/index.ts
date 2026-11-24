import { compile } from "df/core/compilers";
import { IDataformExtension as DataformExtension } from "df/core/extension";
import {
  IJitCompiler as JitCompiler,
  jitCompiler as coreJitCompiler,
  RpcCallback,
} from "df/core/jit_compiler";
import { main as coreMain } from "df/core/main";
import { Session } from "df/core/session";
import { version as coreVersion } from "df/core/version";
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

// These exports constitute the public API of @dataform/core.
// They must also be listed in packages/@dataform/core/index.ts.
// Changes to these will break @dataform/cli, so take care!

/** The installed `@dataform/core` semantic version string (for example, `"3.1.5"`). */
export const version: string = coreVersion;

/** The singleton compilation `Session` instance. */
export const session: Session = globalSession();

/** Transpiles `.sqlx`, `.yaml`/`.yml`, `.sql`, `.ipynb`, and `.md` source strings into executable JavaScript modules. */
export const compiler: (code: string, path: string) => string = compile;

/** Entry point invoked by the compilation VM sandbox; accepts an encoded `dataform.CoreExecutionRequest` proto and returns an encoded `dataform.CoreExecutionResponse` proto containing `CompiledGraph`. */
export const main: (coreExecutionRequest: Uint8Array | string) => Uint8Array | string = coreMain;

/** Creates a Just-in-Time (JiT) action compiler bound to a database adapter `RpcCallback`. */
export const jitCompiler: (rpcCallback: RpcCallback) => IJitCompiler = coreJitCompiler;

/** Feature capability flags supported by this `@dataform/core` version (such as `ARRAY_BUFFER_IPC`). */
export const supportedFeatures: dataform.SupportedFeatures[] = [
  dataform.SupportedFeatures.ARRAY_BUFFER_IPC,
];

/** TypeScript interface for compilation extensions. */
export type IDataformExtension = DataformExtension;

/** TypeScript interface for Just-in-Time (JiT) action compilers. */
export type IJitCompiler = JitCompiler;

/**
 * Older versions of the CLI are not compatible with Core version ^3.0.0, and throw when this
 * method is not available. Instead this more interpretable error message is thrown.
 * Note: for future backwards compatibility breaking changes, the exported "version" variable
 * should be used instead.
 *
 * @internal
 */
export function indexFileGenerator() {
  throw new Error("@dataform/cli ^3.0.0 required.");
}
