import * as protos from "@dataform/protos";

import { ChildProcess, fork } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { promisify } from "util";
import { ICompileIPCParameters, ICompileIPCResult } from "../vm/compile";

export async function compile(compileConfig: protos.ICompileConfig): Promise<protos.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  const projectDir = path.resolve(compileConfig.projectDir);
  const returnedPath = await CompileChildProcess.forkProcess().compile({
    projectDir,
    defaultSchemaOverride: compileConfig.defaultSchemaOverride,
    assertionSchemaOverride: compileConfig.assertionSchemaOverride
  });
  const contents = await promisify(fs.readFile)(returnedPath);
  return protos.CompiledGraph.decode(contents);
}

class CompileChildProcess {
  public static forkProcess() {
    // Run the bin_loader script if inside bazel, otherwise don't.
    const forkScript = process.env.BAZEL_TARGET ? "../vm/compile_bin_loader" : "../vm/compile";
    return new CompileChildProcess(fork(require.resolve(forkScript)));
  }
  private readonly childProcess: ChildProcess;

  constructor(childProcess: ChildProcess) {
    this.childProcess = childProcess;
  }

  public async compile(compileIpcParameters: ICompileIPCParameters) {
    return await new Promise<string>(async (resolve, reject) => {
      this.awaitCompletionOrTimeout();
      this.childProcess.on("message", (result: ICompileIPCResult) => {
        if (!this.childProcess.killed) {
          this.childProcess.kill();
        }
        if (result.err) {
          reject(new Error(result.err));
        } else {
          // We receive back a path where the compiled graph was written in proto format.
          resolve(result.path);
        }
      });
      this.childProcess.send(compileIpcParameters);
    });
  }

  private awaitCompletionOrTimeout() {
    const timeout = 5000;
    const timeoutStart = Date.now();
    const checkTimeout = () => {
      if (this.childProcess.killed) {
        return;
      }
      if (Date.now() > timeoutStart + timeout) {
        this.childProcess.kill();
        throw new Error("Compilation timed out");
      } else {
        setTimeout(checkTimeout, 100);
      }
    };
    checkTimeout();
  }
}
