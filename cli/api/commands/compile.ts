import * as fs from "fs-extra";
import * as path from "path";
import * as tmp from "tmp";
import { promisify } from "util";

import { exec } from "child_process";
import { BaseWorker } from "df/cli/api/commands/base_worker";
import { MISSING_CORE_VERSION_ERROR } from "df/cli/api/commands/install";
import { readDataformCoreVersionFromWorkflowSettings } from "df/cli/api/utils";
import { DEFAULT_COMPILATION_TIMEOUT_MILLIS } from "df/cli/api/utils/constants";
import { coerceAsError } from "df/common/errors/errors";
import { decode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";

export class CompilationTimeoutError extends Error {}

function print(text: string) {
  process.stderr.write(text);
}

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

    if (compileConfig.verbose) {
      print(`Using isolated environment for @dataform/core@${workflowSettingsDataformCoreVersion}\n`);
      print(`Copying project to temporary directory: ${temporaryProjectPath}\n`);
    }
    const copyStartTime = performance.now();
    fs.copySync(resolvedProjectPath, temporaryProjectPath);
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
}`
    );

    const npmCommand = `npm i --ignore-scripts${compileConfig.verbose ? " --loglevel=http" : ""}`;
    if (compileConfig.verbose) {
      print(`Running '${npmCommand}' in temporary directory...\n`);
    }
    const npmStartTime = performance.now();
    const { stdout, stderr } = await promisify(exec)(npmCommand, {
      cwd: temporaryProjectPath
    });
    
    if (compileConfig.verbose) {
      print(`NPM HTTP Logs:\n${stderr}\n`);
      print(`NPM install completed in ${performance.now() - npmStartTime}ms\n`);
    }

    compileConfig.projectDir = temporaryProjectPath;
  }

  const result = await new CompileChildProcess().compile(compileConfig);

  const decodedResult = decode64(dataform.CoreExecutionResponse, result);
  compiledGraph = dataform.CompiledGraph.create(decodedResult.compile.compiledGraph);

  if (workflowSettingsDataformCoreVersion) {
    fs.rmSync(temporaryProjectPath, { recursive: true });
  }

  return compiledGraph;
}

export class CompileChildProcess extends BaseWorker<string, string | Error> {
  constructor() {
    super("../../vm/compile_loader");
  }

  public async compile(compileConfig: dataform.ICompileConfig) {
    const timeoutValue = compileConfig.timeoutMillis || DEFAULT_COMPILATION_TIMEOUT_MILLIS;

    return await this.runWorker(
      timeoutValue,
      child => child.send(compileConfig),
      (message, child, resolve, reject) => {
        if (typeof message === "string") {
          resolve(message);
        } else {
          reject(coerceAsError(message));
        }
      }
    );
  }
}
