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
      const compiledGraphJson = compile(message);

      // AoT compile() in cli/vm/compile.ts returns a JSON string of CompiledGraph.
      // We need to wrap it in a CoreExecutionResponse and Base64 encode it
      // to match what cli/api/commands/compile.ts expects.
      const compiledGraph = dataform.CompiledGraph.create(JSON.parse(compiledGraphJson));
      const response = dataform.CoreExecutionResponse.create({
        compile: { compiledGraph }
      });
      const responseBase64 = Buffer.from(dataform.CoreExecutionResponse.encode(response).finish()).toString("base64");
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
