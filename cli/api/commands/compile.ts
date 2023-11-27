import * as fs from "fs";
import * as path from "path";
import * as semver from "semver";

import { ChildProcess, fork } from "child_process";
import deepmerge from "deepmerge";
import { validWarehouses } from "df/cli/api/dbadapters";
import { coerceAsError, ErrorWithCause } from "df/common/errors/errors";
import { decode64 } from "df/common/protos";
import { setOrValidateTableEnumType } from "df/core/utils";
import { dataform } from "df/protos/ts";

export class CompilationTimeoutError extends Error {}

export async function compile(
  compileConfig: dataform.ICompileConfig = {}
): Promise<dataform.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  path.resolve(compileConfig.projectDir);

  const result = await CompileChildProcess.forkProcess().compile(compileConfig);

  const decodedResult = decode64(dataform.CoreExecutionResponse, result);
  const compileResult = dataform.CompiledGraph.create(decodedResult.compile.compiledGraph);

  compileResult.tables.forEach(setOrValidateTableEnumType);
  return compileResult;
}

export class CompileChildProcess {
  public static forkProcess() {
    // Runs the worker_bundle script we generate for the package (see packages/@dataform/cli/BUILD)
    // if it exists, otherwise run the bazel compile loader target.
    const findForkScript = () => {
      try {
        const workerBundlePath = require.resolve("./worker_bundle");
        return workerBundlePath;
      } catch (e) {
        return require.resolve("../../vm/compile_loader");
      }
    };
    const forkScript = findForkScript();
    return new CompileChildProcess(
      fork(require.resolve(forkScript), [], { stdio: [0, 1, 2, "ipc", "pipe"] })
    );
  }
  private readonly childProcess: ChildProcess;

  constructor(childProcess: ChildProcess) {
    this.childProcess = childProcess;
  }

  public async compile(compileConfig: dataform.ICompileConfig) {
    const compileInChildProcess = new Promise<string>(async (resolve, reject) => {
      this.childProcess.on("error", (e: Error) => reject(coerceAsError(e)));

      this.childProcess.on("message", (messageOrError: string | Error) => {
        if (typeof messageOrError === "string") {
          resolve(messageOrError);
          return;
        }
        reject(coerceAsError(messageOrError));
      });

      this.childProcess.on("close", exitCode => {
        if (exitCode !== 0) {
          reject(new Error(`Compilation child process exited with exit code ${exitCode}.`));
        }
      });

      // Trigger the child process to start compiling.
      this.childProcess.send(compileConfig);
    });
    let timer;
    const timeout = new Promise(
      (resolve, reject) =>
        (timer = setTimeout(
          () => reject(new CompilationTimeoutError("Compilation timed out")),
          compileConfig.timeoutMillis || 5000
        ))
    );
    try {
      await Promise.race([timeout, compileInChildProcess]);
      return await compileInChildProcess;
    } finally {
      if (!this.childProcess.killed) {
        this.childProcess.kill("SIGKILL");
      }
      if (timer) {
        clearTimeout(timer);
      }
    }
  }
}
