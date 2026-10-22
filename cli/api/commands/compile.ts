import { ChildProcess, spawn, spawnSync } from "child_process";
import * as fs from "fs-extra";
import * as path from "path";
import * as tmp from "tmp";

import { BaseWorker } from "df/cli/api/commands/base_worker";
import {
  copyProjectForStatelessInstall,
  explainExcludedModule,
  findProjectIgnoreFiles,
} from "df/cli/api/commands/compile_copy_filter";
import { MISSING_CORE_VERSION_ERROR } from "df/cli/api/commands/install";
import { readConfigFromWorkflowSettings } from "df/cli/api/utils";
import { DEFAULT_COMPILATION_TIMEOUT_MILLIS } from "df/cli/api/utils/constants";
import { coerceAsError } from "df/common/errors/errors";
import { decode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";

export class CompilationTimeoutError extends Error {}

/** Name prefix of the temporary directories created for `dataformCoreVersion` projects. */
export const TEMPORARY_ENVIRONMENT_PREFIX = "dataform-";

/** Prints progress details to stderr when `--verbose` is set. */
type Log = ((text: string) => void) & { readonly verbose: boolean };

function createLog(verbose: boolean): Log {
  const log = (text: string) => {
    if (verbose) {
      process.stderr.write(text);
    }
  };
  return Object.assign(log, { verbose });
}

/**
 * The directory a project is compiled in, i.e. where `@dataform/core` is resolved from
 * `node_modules`. JiT compilation during `dataform run` must use the same directory, so that
 * compilation and JiT use the same `@dataform/core`.
 */
export interface ICompilationEnvironment {
  /** Directory containing the project and its `node_modules/@dataform/core`. */
  projectDir: string;
  /** Removes any temporary files created for this environment. Safe to call more than once. */
  cleanup: () => void;
}

/**
 * Compiles the project and keeps its compilation environment, so that JiT compilation during the
 * run can use the same `@dataform/core`. The caller must call `environment.cleanup()` when done.
 */
export async function compileForRun(compileConfig: dataform.ICompileConfig = {}): Promise<{
  compiledGraph: dataform.CompiledGraph;
  environment: ICompilationEnvironment;
}> {
  const projectDir = path.resolve(compileConfig.projectDir);
  const workflowSettings = readConfigFromWorkflowSettings(projectDir);
  const log = createLog(!!compileConfig.verbose);

  const environment = workflowSettings?.dataformCoreVersion
    ? await createIsolatedEnvironment(projectDir, workflowSettings.dataformCoreVersion, log)
    : useProjectEnvironment(projectDir);
  // A file the copy filter excluded surfaces as a missing module, and the error doesn't
  // say why. Say why alongside the error, which is left as is.
  // This reads the copy, so it runs before the environment is cleaned up.
  const printExcludedModuleNotes = (messages: string[]) => {
    if (environment.projectDir === projectDir) {
      return;
    }
    const notes = new Set<string>();
    for (const message of messages) {
      const note = explainExcludedModule(message, projectDir, environment.projectDir);
      if (note) {
        notes.add(note);
      }
    }
    notes.forEach((note) => process.stderr.write(`Note: ${note}\n`));
  };
  try {
    let result: string;
    try {
      result = await new CompileChildProcess().compile({
        ...compileConfig,
        projectDir: environment.projectDir,
        extension: workflowSettings?.extension ?? undefined,
      });
    } catch (e) {
      if (e instanceof Error) {
        printExcludedModuleNotes([e.message]);
      }
      throw e;
    }
    const decodedResult = decode64(dataform.CoreExecutionResponse, result);
    const compiledGraph = dataform.CompiledGraph.create(decodedResult.compile.compiledGraph);
    printExcludedModuleNotes(
      (compiledGraph.graphErrors?.compilationErrors ?? []).map((error) => error.message ?? ""),
    );
    return { compiledGraph, environment };
  } catch (e) {
    environment.cleanup();
    throw e;
  }
}

export async function compile(
  compileConfig: dataform.ICompileConfig = {},
): Promise<dataform.CompiledGraph> {
  const { compiledGraph, environment } = await compileForRun(compileConfig);
  environment.cleanup();
  return compiledGraph;
}

/** `package.json` projects are compiled in place, with `@dataform/core` from `dataform install`. */
function useProjectEnvironment(projectDir: string): ICompilationEnvironment {
  if (!fs.existsSync(path.join(projectDir, "package.json"))) {
    throw new Error(MISSING_CORE_VERSION_ERROR);
  }
  return { projectDir, cleanup: () => undefined };
}

/**
 * Projects with `dataformCoreVersion` in `workflow_settings.yaml` are copied to a temporary
 * directory where that `@dataform/core` version is installed, so the project directory is left
 * untouched.
 */
async function createIsolatedEnvironment(
  projectDir: string,
  dataformCoreVersion: string,
  log: Log,
): Promise<ICompilationEnvironment> {
  ["node_modules", "package.json", "package-lock.json"]
    .map((npmFile) => path.join(projectDir, npmFile))
    .forEach((npmPath) => {
      if (fs.existsSync(npmPath)) {
        throw new Error(`'${npmPath}' unexpected; remove it and try again`);
      }
    });

  const environment = createTemporaryDirectory();
  try {
    log(`Using isolated environment for @dataform/core@${dataformCoreVersion}\n`);
    log(`Copying project to temporary directory: ${environment.projectDir}\n`);
    if (log.verbose) {
      const ignoreFiles = findProjectIgnoreFiles(projectDir);
      log(
        ignoreFiles.length > 0
          ? `Excluding .git, node_modules, and paths matched by ${ignoreFiles.join(" and ")}, except files reachable from definitions/ and includes/\n`
          : `Excluding .git and node_modules, except files reachable from definitions/ and includes/ (no .gitignore or .dataformignore in project root)\n`,
      );
    }
    const copyStartTime = performance.now();
    copyProjectForStatelessInstall(projectDir, environment.projectDir);
    log(`Project copy completed in ${performance.now() - copyStartTime}ms\n`);

    await installCore(environment, dataformCoreVersion, log);
    return environment;
  } catch (e) {
    environment.cleanup();
    throw e;
  }
}

/** A temporary environment that also stops the processes running in it when cleaned up. */
interface ITemporaryEnvironment extends ICompilationEnvironment {
  /** Runs `command` in a shell in the environment; resolves with its stderr. */
  run(command: string): Promise<string>;
}

function createTemporaryDirectory(): ITemporaryEnvironment {
  const projectDir = tmp.dirSync({ prefix: TEMPORARY_ENVIRONMENT_PREFIX }).name;
  const runningProcesses = new Set<ChildProcess>();
  let removed = false;
  const cleanup = () => {
    if (removed) {
      return;
    }
    removed = true;
    process.removeListener("exit", cleanup);
    // Stop npm first, so that it doesn't keep writing into the directory while it is removed.
    runningProcesses.forEach(killProcessTree);
    // Removal is best effort: a failure to remove the directory mustn't turn a successful
    // compile into a failure, or hide the error that ended the compile.
    try {
      fs.rmSync(projectDir, { recursive: true, force: true, maxRetries: 3 });
    } catch {
      // Left for the OS to clean up with the rest of the temporary directory.
    }
  };
  // Safety net in case the caller never gets to call cleanup(), e.g. on an uncaught exception.
  // Signals skip "exit" unless the process exits explicitly; the CLI does that in
  // handleTerminationSignals (cli/signals.ts).
  process.on("exit", cleanup);

  const run = (command: string) =>
    new Promise<string>((resolve, reject) => {
      const child = spawn(command, {
        cwd: projectDir,
        shell: true,
        // On POSIX, its own process group, so that cleanup can stop the shell and npm together.
        detached: process.platform !== "win32",
        stdio: ["ignore", "ignore", "pipe"],
        windowsHide: true,
      });
      runningProcesses.add(child);
      let stderr = "";
      child.stderr.on("data", (chunk) => (stderr += String(chunk)));
      child.on("error", (e) => reject(Object.assign(e, { stderr })));
      child.on("close", (code, signal) => {
        runningProcesses.delete(child);
        if (code === 0) {
          resolve(stderr);
        } else {
          const status = signal ? `signal ${signal}` : `code ${code}`;
          reject(Object.assign(new Error(`'${command}' exited with ${status}`), { stderr }));
        }
      });
    });

  return { projectDir, cleanup, run };
}

function killProcessTree(child: ChildProcess) {
  try {
    if (process.platform === "win32") {
      spawnSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], { windowsHide: true });
    } else {
      // SIGKILL takes effect immediately, so nothing writes into the directory afterwards.
      process.kill(-child.pid, "SIGKILL");
    }
  } catch (e) {
    // The process has already exited.
  }
}

async function installCore(
  environment: ITemporaryEnvironment,
  dataformCoreVersion: string,
  log: Log,
) {
  log(`Generating temporary package.json\n`);
  fs.writeFileSync(
    path.join(environment.projectDir, "package.json"),
    JSON.stringify({ dependencies: { "@dataform/core": dataformCoreVersion } }, null, 2),
  );

  const npmCommand = `npm i --ignore-scripts${log.verbose ? " --loglevel=http" : ""}`;
  log(`Running '${npmCommand}' in temporary directory...\n`);
  const npmStartTime = performance.now();
  try {
    const stderr = await environment.run(npmCommand);
    log(`NPM HTTP Logs:\n${stderr}\n`);
    log(`NPM install completed in ${performance.now() - npmStartTime}ms\n`);
  } catch (e) {
    const details = String(e?.stderr || "").trim() || e?.message || String(e);
    throw new Error(
      `Failed to install @dataform/core@${dataformCoreVersion} (dataformCoreVersion in ` +
        `workflow_settings.yaml). Check that the version exists and that the npm registry is ` +
        `reachable.\n${details}`,
    );
  }
}

export class CompileChildProcess extends BaseWorker<string, string | Error> {
  public async compile(compileConfig: dataform.ICompileConfig) {
    const timeoutValue = compileConfig.timeoutMillis || DEFAULT_COMPILATION_TIMEOUT_MILLIS;

    return await this.runWorker(
      timeoutValue,
      (child) => child.send(compileConfig),
      (message, child, resolve, reject) => {
        if (typeof message === "string") {
          resolve(message);
          return;
        }
        reject(coerceAsError(message));
      },
    );
  }
}
