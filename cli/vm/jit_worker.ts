import * as fs from "fs";
import * as path from "path";
import { NodeVM } from "vm2";

import { dataform } from "df/protos/ts";

const pendingRpcCallbacks = new Map<string, (err: string | null, resBytes: Uint8Array | null) => void>();

// Guard against double-initialization in some environments (e.g. Bazel)
const globalObj = global as any;
if (!globalObj._dataform_jit_worker_initialized) {
  globalObj._dataform_jit_worker_initialized = true;
  globalObj._has_started_processing = false;

  process.on("message", (res: any) => {
    if (res.type === "rpc_response") {
      const callback = pendingRpcCallbacks.get(res.correlationId);
      if (callback) {
        pendingRpcCallbacks.delete(res.correlationId);
        if (res.error) {
          callback(res.error, null);
        } else {
          callback(null, res.response);
        }
      }
    }
  });

  if (require.main === module || globalObj._dataform_jit_worker_force_main) {
    if (process.send) {
      process.send({ type: "worker_booted" });
    }
    process.on("message", async (message: any) => {
      if (message.type === "jit_compile") {
        if (globalObj._has_started_processing) {
          process.send({
            type: "jit_error",
            error: "Worker process received multiple JiT compilation requests. Subsequent requests are rejected."
          });
          return;
        }
        globalObj._has_started_processing = true;
        await handleJitRequest(message);
      }
    });
  }
}

export async function handleJitRequest(message: {
  request: any;
  projectDir: string;
}) {
  try {
    const { request, projectDir } = message;

    const projectLocalCorePath = path.join(projectDir, "node_modules", "@dataform", "core", "bundle.js");
    const hasProjectLocalCore = fs.existsSync(projectLocalCorePath);

    if (!hasProjectLocalCore && !fs.existsSync(path.join(projectDir, "node_modules", "@dataform", "core", "package.json"))) {
      throw new Error(
        "Could not find a recent installed version of @dataform/core in the project. Check that " +
          "either `dataformCoreVersion` is specified in `workflow_settings.yaml`, or " +
          "`@dataform/core` is specified in `package.json`. If using `package.json`, then run " +
          "`dataform install`."
      );
    }

    const rpcCallback = (method: string, reqBytes: Uint8Array, callback: (err: string | null, resBytes: Uint8Array | null) => void) => {
      const correlationId = Math.random().toString(36).substring(7);
      pendingRpcCallbacks.set(correlationId, callback);

      process.send({
        type: "rpc_request",
        method,
        request: reqBytes,
        correlationId
      });
    };

    const requestMessage = dataform.JitCompilationRequest.fromObject(request);
    const requestBytes = dataform.JitCompilationRequest.encode(requestMessage).finish();

    // Use the action's file name as the VM filename for correct relative requires.
    const vmFileName = requestMessage.fileName
      ? path.resolve(projectDir, requestMessage.fileName)
      : path.resolve(projectDir, "index.js");

    const vm = new NodeVM({
      env: process.env,
      require: {
        builtin: ["path", "fs"],
        context: "sandbox",
        external: true,
        root: projectDir,
        mock: hasProjectLocalCore ? {} : {
          "@dataform/core": require("@dataform/core")
        },
        resolve: (moduleName, parentDirName) => {
          if (moduleName.startsWith(".")) {
            return path.resolve(parentDirName, moduleName);
          }
          return moduleName;
        }
      },
      sourceExtensions: ["js", "json"]
    });

    const jitCompileInVm = vm.run(`
      const { jitCompiler } = require("@dataform/core");

      global.require = require;

      module.exports = async (requestBytes, armoredRpcCallback) => {
        const requestBytesTyped = new Uint8Array(requestBytes);
        const internalRpcCallback = (method, reqBytes, callback) => {
           armoredRpcCallback(method, Buffer.from(reqBytes), (errStr, resBytes) => {
              if (errStr) {
                callback(new Error(errStr), null);
              } else {
                callback(null, new Uint8Array(resBytes));
              }
           });
        };

        const compilerInstance = jitCompiler(internalRpcCallback);
        return await compilerInstance.compile(requestBytesTyped);
      };
    `, vmFileName);

    const responseBytes = await jitCompileInVm(requestBytes, rpcCallback);
    const response = dataform.JitCompilationResponse.decode(Buffer.from(responseBytes));

    process.send({ type: "jit_response", response: response.toJSON() });
  } catch (e) {
    process.send({ type: "jit_error", error: e.stack || e.message });
  }
}
