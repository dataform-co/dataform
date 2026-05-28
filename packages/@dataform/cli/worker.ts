import { listenForCompileRequest } from "df/cli/vm/compile";
import { handleJitRequest } from "df/cli/vm/jit_worker";

process.on("message", async (message: any) => {
  if (message?.type === "jit_compile") {
    await handleJitRequest(message);
  }
});

listenForCompileRequest();

if (process.send) {
  process.send({ type: "worker_booted" });
}
