import * as path from "path";

import { findInstalledCoreBundle } from "df/cli/vm/core_installation";
import { VmRunner } from "df/common/vm/vm_runner";
import { dataform } from "df/protos/ts";

const pendingRpcCallbacks = new Map<
  string,
  (err: string | null, resBytes: Uint8Array | null) => void
>();

export function registerRpcResponseHandler() {
  process.on("message", (res: any) => {
    if (res.type === "rpc_response") {
      const callback = pendingRpcCallbacks.get(res.correlationId);
      if (callback) {
        pendingRpcCallbacks.delete(res.correlationId);
        callback(res.error || null, res.response ? Buffer.from(res.response) : null);
      }
    }
  });
}

export function registerJitCompileHandler() {
  let hasStartedProcessing = false;
  process.on("message", async (message: any) => {
    if (message.type === "jit_compile") {
      if (hasStartedProcessing) {
        process.send({
          type: "jit_error",
          error:
            "Worker process received multiple JiT compilation requests. Subsequent requests are rejected.",
        });
        return;
      }
      hasStartedProcessing = true;
      await handleJitRequest(message);
    }
  });
}

export interface IJitCompileMessage {
  request: any;
  /** Directory whose `node_modules/@dataform/core` is used for JiT compilation. */
  projectDir: string;
  /** The `@dataform/core` version that compiled the project. JiT must use the same version. */
  expectedCoreVersion?: string;
}

export async function handleJitRequest(message: IJitCompileMessage) {
  try {
    const { request, projectDir, expectedCoreVersion } = message;

    // Always use the project's own @dataform/core, never the copy bundled with the CLI: JiT must
    // use the same core version as compilation.
    const coreBundlePath = findInstalledCoreBundle(projectDir);

    const rpcCallback = (
      method: string,
      reqBytes: Uint8Array,
      callback: (err: string | null, resBytes: Uint8Array | null) => void,
    ) => {
      const correlationId = Math.random().toString(36).substring(7);
      pendingRpcCallbacks.set(correlationId, callback);

      process.send({
        type: "rpc_request",
        method,
        request: reqBytes,
        correlationId,
      });
    };

    const requestMessage = dataform.JitCompilationRequest.fromObject(request);
    const requestBytes = Array.from(dataform.JitCompilationRequest.encode(requestMessage).finish());

    const vmFileName = path.resolve(projectDir, "index.js");

    const vm = new VmRunner({
      projectDir,
      builtinModules: [],
      allowedModules: ["@dataform/*"],
      sourceExtensions: ["js", "json", "yaml", "yml"],
    });

    const {
      version: coreVersion,
      hasJitCompiler,
      compile: jitCompileInVm,
    } = vm.run(
      `
      const core = require("@dataform/core");

      global.require = require;

      return {
        version: core.version,
        hasJitCompiler: typeof core.jitCompiler === "function",
        compile: async (requestBytes, armoredRpcCallback) => {
          const requestBytesTyped = new Uint8Array(requestBytes);
          const internalRpcCallback = (method, reqBytes, callback) => {
            armoredRpcCallback(method, Array.from(reqBytes), (errStr, resBytes) => {
              if (errStr) {
                return callback(new Error(errStr));
              }
              callback(null, resBytes ? new Uint8Array(resBytes) : new Uint8Array());
            });
          };

          const compilerInstance = core.jitCompiler(internalRpcCallback);
          return await compilerInstance.compile(requestBytesTyped);
        },
      };
    `,
      vmFileName,
    );
    const coreDescription = coreVersion
      ? `@dataform/core ${coreVersion}`
      : "@dataform/core of an unknown version";
    if (expectedCoreVersion && coreVersion !== expectedCoreVersion) {
      throw new Error(
        `JiT compilation would use ${coreDescription} from '${coreBundlePath}', but the ` +
          `project was compiled with @dataform/core ${expectedCoreVersion}. Compilation and JiT ` +
          `compilation must use the same @dataform/core version.`,
      );
    }
    if (!hasJitCompiler) {
      throw new Error(
        `${coreDescription} does not support JiT compilation. Upgrade ` +
          `@dataform/core to a version that supports JiT.`,
      );
    }

    const responseBytes = await jitCompileInVm(requestBytes, rpcCallback);
    const response = dataform.JitCompilationResponse.decode(
      new Uint8Array(responseBytes as number[]),
    );

    process.send({ type: "jit_response", response: response.toJSON() });
  } catch (e) {
    process.send({ type: "jit_error", error: e.stack || e.message });
  }
}
