import * as fs from "fs";
import * as path from "path";
import { NodeVM } from "vm2";

import { dataform } from "df/protos/ts";

const pendingRpcCallbacks = new Map<string, (err: string | null, resBase64: string | null) => void>();

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

    const rpcCallback = (method: string, reqBase64: string, callback: (err: string | null, resBase64: string | null) => void) => {
      const correlationId = Math.random().toString(36).substring(7);
      pendingRpcCallbacks.set(correlationId, callback);

      process.send({
        type: "rpc_request",
        method,
        request: reqBase64,
        correlationId
      });
    };

    const projectLocalCorePath = path.join(projectDir, "node_modules", "@dataform", "core", "bundle.js");
    const hasProjectLocalCore = fs.existsSync(projectLocalCorePath);

    const requestMessage = dataform.JitCompilationRequest.fromObject(request);
    const requestBytes = dataform.JitCompilationRequest.encode(requestMessage).finish();
    const requestBase64 = Buffer.from(requestBytes).toString("base64");

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

      module.exports = async (requestBase64, armoredRpcCallback) => {
        const requestBytes = new Uint8Array(Buffer.from(requestBase64, "base64"));

        const internalRpcCallback = (method, reqBytes, callback) => {
           const reqBase64 = Buffer.from(reqBytes).toString("base64");
           armoredRpcCallback(method, reqBase64, (errStr, resBase64) => {
              if (errStr) {
                callback(new Error(errStr), null);
              } else {
                callback(null, new Uint8Array(Buffer.from(resBase64, "base64")));
              }
           });
        };

        const compilerInstance = jitCompiler(internalRpcCallback);
        const responseBytes = await compilerInstance.compile(requestBytes);

        return Buffer.from(responseBytes).toString("base64");
      };
    `, vmFileName);

    const responseBase64 = await jitCompileInVm(requestBase64, rpcCallback);
    const response = dataform.JitCompilationResponse.decode(Buffer.from(responseBase64, "base64"));

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
      await handleJitRequest(message);
    }
  });
}
