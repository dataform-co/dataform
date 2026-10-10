import { exec } from "child_process";
import * as fs from "fs-extra";
import * as path from "path";
import * as tmp from "tmp";
import { promisify } from "util";

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

function print(text: string) {
  process.stderr.write(text);
}

export async function compile(
  compileConfig: dataform.ICompileConfig = {},
): Promise<dataform.CompiledGraph> {
  let compiledGraph = dataform.CompiledGraph.create();

  const resolvedProjectPath = path.resolve(compileConfig.projectDir);
  const packageJsonPath = path.join(resolvedProjectPath, "package.json");
  const packageLockJsonPath = path.join(resolvedProjectPath, "package-lock.json");
  const projectNodeModulesPath = path.join(resolvedProjectPath, "node_modules");

  const temporaryProjectPath = tmp.dirSync().name;

  const workflowSettings = readConfigFromWorkflowSettings(resolvedProjectPath);
  const workflowSettingsDataformCoreVersion = workflowSettings?.dataformCoreVersion;
  const workflowSettingsExtension = workflowSettings?.extension ?? undefined;

  compileConfig.extension = workflowSettingsExtension;

  if (!workflowSettingsDataformCoreVersion && !fs.existsSync(packageJsonPath)) {
    throw new Error(MISSING_CORE_VERSION_ERROR);
  }

  // For stateless package installation, a temporary directory is used in order to avoid interfering
  // with user's project directories.
  if (workflowSettingsDataformCoreVersion) {
    [projectNodeModulesPath, packageJsonPath, packageLockJsonPath].forEach((npmPath) => {
      if (fs.existsSync(npmPath)) {
        throw new Error(`'${npmPath}' unexpected; remove it and try again`);
      }
    });

    if (compileConfig.verbose) {
      print(
        `Using isolated environment for @dataform/core@${workflowSettingsDataformCoreVersion}\n`,
      );
      print(`Copying project to temporary directory: ${temporaryProjectPath}\n`);
      const ignoreFiles = findProjectIgnoreFiles(resolvedProjectPath);
      print(
        ignoreFiles.length > 0
          ? `Excluding .git, node_modules, and paths matched by ${ignoreFiles.join(" and ")}, except files reachable from definitions/ and includes/\n`
          : `Excluding .git and node_modules, except files reachable from definitions/ and includes/ (no .gitignore or .dataformignore in project root)\n`,
      );
    }
    const copyStartTime = performance.now();
    copyProjectForStatelessInstall(resolvedProjectPath, temporaryProjectPath);
    if (compileConfig.verbose) {
      print(`Project copy completed in ${performance.now() - copyStartTime}ms\n`);
    }

    if (compileConfig.verbose) {
      print(`Generating temporary package.json\n`);
    }
    fs.writeFileSync(
      path.join(temporaryProjectPath, "package.json"),
      `{
  "dependencies": {
  "@dataform/core": "${workflowSettingsDataformCoreVersion}"
  }
}`,
    );

    const npmCommand = `npm i --ignore-scripts${compileConfig.verbose ? " --loglevel=http" : ""}`;
    if (compileConfig.verbose) {
      print(`Running '${npmCommand}' in temporary directory...\n`);
    }
    const npmStartTime = performance.now();
    const { stdout, stderr } = await promisify(exec)(npmCommand, {
      cwd: temporaryProjectPath,
    });

    if (compileConfig.verbose) {
      print(`NPM HTTP Logs:\n${stderr}\n`);
      print(`NPM install completed in ${performance.now() - npmStartTime}ms\n`);
    }

    compileConfig.projectDir = temporaryProjectPath;
  }

  // A file the copy filter excluded surfaces as a missing module, which the CLI can
  // report as a missing npm dependency. Say why alongside the error, which is left as is.
  const printExcludedModuleNotes = (messages: string[]) => {
    const notes = new Set<string>();
    for (const message of messages) {
      const note = explainExcludedModule(message, resolvedProjectPath, temporaryProjectPath);
      if (note) {
        notes.add(note);
      }
    }
    notes.forEach((note) => print(`Note: ${note}\n`));
  };

  let result: string;
  try {
    result = await new CompileChildProcess().compile(compileConfig);
  } catch (e) {
    if (workflowSettingsDataformCoreVersion && e instanceof Error) {
      printExcludedModuleNotes([e.message]);
    }
    throw e;
  }

  const decodedResult = decode64(dataform.CoreExecutionResponse, result);
  compiledGraph = dataform.CompiledGraph.create(decodedResult.compile.compiledGraph);

  if (workflowSettingsDataformCoreVersion) {
    printExcludedModuleNotes(
      (compiledGraph.graphErrors?.compilationErrors ?? []).map((error) => error.message ?? ""),
    );
    fs.rmSync(temporaryProjectPath, { recursive: true });
  }

  return compiledGraph;
}

export class CompileChildProcess extends BaseWorker<string, string | Error> {
  constructor() {
    super(path.resolve(__dirname, "../../vm/compile_loader"));
  }

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
