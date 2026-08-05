import { listenForCompileRequest } from "df/cli/vm/compile";
import { registerJitCompileHandler, registerRpcResponseHandler } from "df/cli/vm/jit_worker";

registerRpcResponseHandler();
registerJitCompileHandler();
listenForCompileRequest();

if (process.send) {
  process.send({ type: "worker_booted" });
}
