import { validate } from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { ChildProcess, fork } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { util } from "protobufjs";

export async function compile(
  compileConfig: dataform.ICompileConfig
): Promise<dataform.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  path.resolve(compileConfig.projectDir);

  try {
    // check dataformJson is valid before we try to compile
    const dataformJson = fs.readFileSync(`${compileConfig.projectDir}/dataform.json`, "utf8");
    JSON.parse(dataformJson);
  } catch (e) {
    throw new Error("Compile Error: `dataform.json` is invalid");
  }

  const compiledGraph = await CompileChildProcess.forkProcess().compile(compileConfig);
  return dataform.CompiledGraph.create({
    ...compiledGraph,
    graphErrors: validate(compiledGraph)
  });
}

class CompileChildProcess {
  public static forkProcess() {
    // Run the bin_loader script if inside bazel, otherwise don't.
    const forkScript = process.env.BAZEL_TARGET ? "../vm/compile_bin_loader" : "../vm/compile";
    return new CompileChildProcess(
      fork(require.resolve(forkScript), [], { stdio: [0, 1, 2, "ipc", "pipe"] })
    );
  }
  private readonly childProcess: ChildProcess;

  constructor(childProcess: ChildProcess) {
    this.childProcess = childProcess;
  }

  public async compile(compileConfig: dataform.ICompileConfig) {
    const compileInChildProcess = new Promise<dataform.CompiledGraph>(async (resolve, reject) => {
      // Handle errors returned by the child process.
      this.childProcess.on("message", (e: Error) => reject(e));

      // Handle CompiledGraphs returned by the child process.
      const pipe = this.childProcess.stdio[4];
      const chunks: Buffer[] = [];
      pipe.on("data", (chunk: Buffer) => chunks.push(chunk));
      pipe.on("end", () => {
        // The child process returns a base64 encoded proto.
        const allData = Buffer.concat(chunks).toString("utf8");
        const encodedGraphBytes = new Uint8Array(util.base64.length(allData));
        util.base64.decode(allData, encodedGraphBytes, 0);
        resolve(dataform.CompiledGraph.decode(encodedGraphBytes));
      });

      // Trigger the child process to start compiling.
      this.childProcess.send(compileConfig);
    });
    let timer;
    const timeout = new Promise(
      (resolve, reject) =>
        (timer = setTimeout(() => reject(new Error("Compilation timed out")), 5000))
    );
    try {
      await Promise.race([timeout, compileInChildProcess]);
      return await compileInChildProcess;
    } finally {
      if (!this.childProcess.killed) {
        this.childProcess.kill();
      }
      if (timer) {
        clearTimeout(timer);
      }
    }
  }
}
