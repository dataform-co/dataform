import { compile } from "df/cli/vm/compile";
import { handleJitRequest } from "df/cli/vm/jit_worker";

process.on("message", async (message: any) => {
  try {
    if (message.type === "jit_compile") {
      await handleJitRequest(message);
    } else {
      // It's an AoT compile request
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
