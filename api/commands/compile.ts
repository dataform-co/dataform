import * as fs from "fs";
import * as path from "path";

import { ChildProcess, fork } from "child_process";
import deepmerge from "deepmerge";
import { validWarehouses } from "df/api/dbadapters";
import { coerceAsError, ErrorWithCause } from "df/common/errors/errors";
import { decode } from "df/common/protos";
import { dataform } from "df/protos/ts";

// Project config properties that are required.
const mandatoryProps: Array<keyof dataform.IProjectConfig> = ["warehouse", "defaultSchema"];

// Project config properties that require alphanumeric characters, hyphens or underscores.
const simpleCheckProps: Array<keyof dataform.IProjectConfig> = [
  "assertionSchema",
  "databaseSuffix",
  "schemaSuffix",
  "tablePrefix",
  "defaultSchema"
];

export class CompilationTimeoutError extends Error {}

export async function compile(
  compileConfig: dataform.ICompileConfig = {}
): Promise<dataform.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  path.resolve(compileConfig.projectDir);

  // Create an empty projectConfigOverride if not set.
  compileConfig = { projectConfigOverride: {}, ...compileConfig };

  // Schema overrides field can be set in two places, projectConfigOverride.schemaSuffix takes precedent.
  if (compileConfig.schemaSuffixOverride) {
    compileConfig.projectConfigOverride = {
      schemaSuffix: compileConfig.schemaSuffixOverride,
      ...compileConfig.projectConfigOverride
    };
  }

  try {
    // check dataformJson is valid before we try to compile
    const dataformJson = fs.readFileSync(`${compileConfig.projectDir}/dataform.json`, "utf8");
    const projectConfig = JSON.parse(dataformJson);
    checkDataformJsonValidity(deepmerge(projectConfig, compileConfig.projectConfigOverride));
  } catch (e) {
    throw new ErrorWithCause(
      `Compilation failed. ProjectConfig ('dataform.json') is invalid: ${e.message}`,
      e
    );
  }

  return decode(
    dataform.CompiledGraph,
    await CompileChildProcess.forkProcess().compile(compileConfig)
  );
}

export class CompileChildProcess {
  public static forkProcess() {
    let workerBundle: string;
    try {
      // The bundled CLI packages the worker_bundle directly.
      workerBundle = require.resolve("./worker_bundle");
    } catch (e) {
      try {
        // This resolution  happens when run in this Bazel environment. It could be avoided by copying
        // the worker bundle via bazel to the appropriate places for every use case, but this seems cleaner.
        workerBundle = require.resolve("df/sandbox/vm/worker_bundle.js");
      } catch (e) {
        // This resolution happens when run in an external bazel workspace. The worker bundle must explicity
        // be copied to the root of of the project by a build rule.
        workerBundle = "./bazel-bin/worker_bundle/worker_bundle.js";
      }
    }
    return new CompileChildProcess(
      fork(workerBundle, [], {
        stdio: [0, 1, 2, "ipc", "pipe"]
      })
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
