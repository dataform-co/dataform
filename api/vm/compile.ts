import * as path from "path";
import { NodeVM } from "vm2";
import { compilers } from "@dataform/core";
import * as protos from "@dataform/protos";
import { genIndex } from "../gen_index";
import * as fs from "fs";
import * as os from "os";
import * as crypto from "crypto";

export function compile(projectDir: string): protos.CompiledGraph {
  const vm = new NodeVM({
    wrapper: "none",
    require: {
      context: "sandbox",
      root: projectDir,
      external: true
    },
    sourceExtensions: ["js", "sql"],
    compiler: (code, path) => compilers.compile(code, path)
  });

  const indexScript = genIndex(projectDir);
  const result = vm.run(indexScript, path.resolve(path.join(projectDir, "index.js")));
  const buf = new Uint8Array(result);

  return protos.CompiledGraph.decode(buf);
}

process.on("message", object => {
  try {
    // IPC breaks down above 200kb, which is a problem. Instead, pass via file system...
    // TODO: This isn't ideal.
    const tmpDir = path.join(os.tmpdir(), "dataform");
    if (!fs.existsSync(tmpDir)) {
      fs.mkdirSync(tmpDir);
    }
    // Create a consistent hash for the temporary path based on the absolute project path.
    const absProjectPathHash = crypto
      .createHash("md5")
      .update(path.resolve(object.projectDir))
      .digest("hex");
    const tmpPath = path.join(tmpDir, absProjectPathHash);
    // Clear the transfer path before writing it.
    if (fs.existsSync(tmpPath)) {
      fs.unlinkSync(tmpPath);
    }
    const graph = compile(object.projectDir);
    // Use protobuffer encoding rather than JSON.
    const encodedGraph = protos.CompiledGraph.encode(graph).finish();
    fs.writeFileSync(tmpPath, encodedGraph);
    // Send back the temp path.
    process.send({ path: String(tmpPath) });
  } catch (e) {
    console.log(e);
    process.send({ err: String(e) });
  }
  process.exit();
});
