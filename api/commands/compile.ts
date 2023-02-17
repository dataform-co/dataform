import * as fs from "fs";
import * as path from "path";
import * as semver from "semver";

import { ChildProcess, fork } from "child_process";
import deepmerge from "deepmerge";
import { validWarehouses } from "df/api/dbadapters";
import { coerceAsError, ErrorWithCause } from "df/common/errors/errors";
import { decode64 } from "df/common/protos";
import { setOrValidateTableEnumType } from "df/core/utils";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

// Project config properties that are required.
const mandatoryProps: Array<keyof core.ProjectConfig> = ["warehouse", "defaultSchema"];

// Project config properties that require alphanumeric characters, hyphens or underscores.
const simpleCheckProps: Array<keyof core.ProjectConfig> = [
  "assertionSchema",
  "databaseSuffix",
  "schemaSuffix",
  "tablePrefix",
  "defaultSchema"
];

export class CompilationTimeoutError extends Error {}

export async function compile(
  compileConfig: dataform.CompileConfig = {}
): Promise<core.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  path.resolve(compileConfig.projectDir);

  try {
    // check dataformJson is valid before we try to compile
    const dataformJson = fs.readFileSync(`${compileConfig.projectDir}/dataform.json`, "utf8");
    const projectConfig = JSON.parse(dataformJson);
    checkDataformJsonValidity(deepmerge(projectConfig, compileConfig.projectConfigOverride || {}));
  } catch (e) {
    throw new ErrorWithCause(
      `Compilation failed. ProjectConfig ('dataform.json') is invalid: ${e.message}`,
      e
    );
  }

  if (compileConfig.useMain === null || compileConfig.useMain === undefined) {
    try {
      const packageJson = JSON.parse(
        fs.readFileSync(`${compileConfig.projectDir}/package.json`, "utf8")
      );
      const dataformCoreVersion = packageJson.dependencies["@dataform/core"];
      compileConfig.useMain = semver.subset(dataformCoreVersion, ">=2.0.4");
    } catch (e) {
      // Silently catch any thrown Error. Do not attempt to use `main` compilation.
    }
  }

  const result = await CompileChildProcess.forkProcess().compile(compileConfig);

  let compileResult: core.CompiledGraph;
  if (compileConfig.useMain) {
    const decodedResult = decode64(dataform.CoreExecutionResponse, result);
    compileResult = core.CompiledGraph.create(decodedResult.compile.compiledGraph);
  } else {
    compileResult = decode64(core.CompiledGraph, result);
  }

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
        return require.resolve("../../sandbox/vm/compile_loader");
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

  public async compile(compileConfig: dataform.CompileConfig) {
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

export const checkDataformJsonValidity = (dataformJsonParsed: { [prop: string]: any }) => {
  const invalidWarehouseProp = () => {
    return dataformJsonParsed.warehouse && !validWarehouses.includes(dataformJsonParsed.warehouse)
      ? `Invalid value on property warehouse: ${
          dataformJsonParsed.warehouse
        }. Should be one of: ${validWarehouses.join(", ")}.`
      : null;
  };
  const invalidProp = () => {
    const invProp = simpleCheckProps.find(prop => {
      return prop in dataformJsonParsed && !/^[a-zA-Z_0-9\-]*$/.test(dataformJsonParsed[prop]);
    });
    return invProp
      ? `Invalid value on property ${invProp}: ${dataformJsonParsed[invProp]}. Should only contain alphanumeric characters, underscores and/or hyphens.`
      : null;
  };
  const missingMandatoryProp = () => {
    const missMandatoryProp = mandatoryProps.find(prop => {
      return !(prop in dataformJsonParsed);
    });
    return missMandatoryProp ? `Missing mandatory property: ${missMandatoryProp}.` : null;
  };
  const message = invalidWarehouseProp() || invalidProp() || missingMandatoryProp();
  if (message) {
    throw new Error(message);
  }
};
