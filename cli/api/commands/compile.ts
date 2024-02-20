import * as fs from "fs-extra";
import * as path from "path";
import * as tmp from "tmp";
import { promisify } from "util";

import { ChildProcess, exec, fork } from "child_process";
import { MISSING_CORE_VERSION_ERROR } from "df/cli/api/commands/install";
import { readDataformCoreVersionFromWorkflowSettings } from "df/cli/api/utils";
import { coerceAsError } from "df/common/errors/errors";
import { decode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";

export class CompilationTimeoutError extends Error {}

export async function compile(
  compileConfig: dataform.ICompileConfig = {}
): Promise<dataform.CompiledGraph> {
  let compiledGraph = dataform.CompiledGraph.create();

  const resolvedProjectPath = path.resolve(compileConfig.projectDir);
  const packageJsonPath = path.join(resolvedProjectPath, "package.json");
  const packageLockJsonPath = path.join(resolvedProjectPath, "package-lock.json");
  const projectNodeModulesPath = path.join(resolvedProjectPath, "node_modules");

  const temporaryProjectPath = tmp.dirSync().name;

  const workflowSettingsDataformCoreVersion = readDataformCoreVersionFromWorkflowSettings(
    resolvedProjectPath
  );

  if (!workflowSettingsDataformCoreVersion && !fs.existsSync(packageJsonPath)) {
    throw new Error(MISSING_CORE_VERSION_ERROR);
  }

  // For stateless package installation, a temporary directory is used in order to avoid interfering
  // with user's project directories.
  if (workflowSettingsDataformCoreVersion) {
    [projectNodeModulesPath, packageJsonPath, packageLockJsonPath].forEach(npmPath => {
      if (fs.existsSync(npmPath)) {
        throw new Error(`'${npmPath}' unexpected; remove it and try again`);
      }
    });

    fs.copySync(resolvedProjectPath, temporaryProjectPath);

    fs.writeFileSync(
      path.join(temporaryProjectPath, "package.json"),
      `{
  "dependencies": {
  "@dataform/core": "${workflowSettingsDataformCoreVersion}"
  }
}`
    );

    await promisify(exec)("npm i --ignore-scripts", {
      cwd: temporaryProjectPath
    });

    compileConfig.projectDir = temporaryProjectPath;
  }

  const result = await CompileChildProcess.forkProcess().compile(compileConfig);

  const decodedResult = decode64(dataform.CoreExecutionResponse, result);
  compiledGraph = dataform.CompiledGraph.create(decodedResult.compile.compiledGraph);

  if (workflowSettingsDataformCoreVersion) {
    fs.rmdirSync(temporaryProjectPath, { recursive: true });
  }

  return compiledGraph;
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
