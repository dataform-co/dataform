import { compile } from "df/cli/vm/compile";
import { handleJitRequest } from "df/cli/vm/jit_worker";
import { dataform } from "df/protos/ts";

process.on("message", async (message: any) => {
  try {
    if (message.request) {
      // It's a JiT request
      await handleJitRequest(message);
    } else {
      // It's an AoT compile request
      // compile() in cli/vm/compile.ts returns a base64-encoded CoreExecutionResponse string.
      const responseBase64 = compile(message);
      process.send(responseBase64);
    }
  } catch (e) {
    process.send({
       error: e.message || String(e),
       stack: e.stack,
       name: e.name
    });
  }
});

// Signal that the worker is alive and listening
if (process.send) {
  process.send({ type: "worker_booted" });
}
