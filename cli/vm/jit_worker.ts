import * as fs from "fs";
import * as path from "path";
import { NodeVM } from "vm2";

import { dataform } from "df/protos/ts";

const pendingRpcCallbacks = new Map<string, (err: string | null, resBytes: Uint8Array | null) => void>();
let hasStartedProcessing = false;

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

export async function handleJitRequest(message: {
  request: any;
  projectDir: string;
}) {
  try {
    const { request, projectDir } = message;

    if (!fs.existsSync(path.join(projectDir, "node_modules", "@dataform", "core", "bundle.js"))) {
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

    const vmFileName = path.resolve(projectDir, "index.js");

    const vm = new NodeVM({
      env: process.env,
      require: {
        builtin: [],
        context: "sandbox",
        external: {
          modules: ["@dataform/*"]
        },
        root: projectDir
      },
      sourceExtensions: ["js", "json"]
    });

    const jitCompileInVm = vm.run(`
      const { jitCompiler } = require("@dataform/core");

      global.require = require;

      module.exports = async (requestBytes, armoredRpcCallback) => {
        const internalRpcCallback = (method, reqBytes, callback) => {
           armoredRpcCallback(method, reqBytes, (errStr, resBytes) => {
              if (errStr) {
                callback(new Error(errStr), null);
              } else {
                callback(null, resBytes);
              }
           });
        };

        const compilerInstance = jitCompiler(internalRpcCallback);
        return await compilerInstance.compile(requestBytes);
      };
    `, vmFileName);

    const responseBytes = await jitCompileInVm(requestBytes, rpcCallback);
    const response = dataform.JitCompilationResponse.decode(responseBytes);

    process.send({ type: "jit_response", response: response.toJSON() });
  } catch (e) {
    process.send({ type: "jit_error", error: e.stack || e.message });
  }
}

if (require.main === module) {
  if (process.send) {
    process.send({ type: "worker_booted" });
  }
  process.on("message", async (message: any) => {
    if (message.request) {
      if (hasStartedProcessing) {
        process.send({
          type: "jit_error",
          error: "Worker process received multiple JiT compilation requests. Subsequent requests are rejected."
        });
        return;
      }
      hasStartedProcessing = true;
      await handleJitRequest(message);
    }
  });
}
