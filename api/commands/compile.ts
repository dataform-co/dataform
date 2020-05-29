import { ChildProcess, fork } from "child_process";
import { ErrorWithCause } from "df/common/errors/errors";
import { validate } from "df/core/utils";
import { dataform } from "df/protos/ts";
import * as fs from "fs";
import * as path from "path";
import { util } from "protobufjs";

const validWarehouses = ["bigquery", "postgres", "redshift", "sqldatawarehouse", "snowflake"];
const mandatoryProps: Array<keyof dataform.IProjectConfig> = ["warehouse", "defaultSchema"];
const simpleCheckProps: Array<keyof dataform.IProjectConfig> = [
  "assertionSchema",
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
    checkDataformJsonValidity({
      ...projectConfig,
      ...compileConfig.projectConfigOverride
    });
  } catch (e) {
    throw new ErrorWithCause("Compilation failed: ProjectConfig ('dataform.json') is invalid.", e);
  }

  const encodedGraphInBase64 = await CompileChildProcess.forkProcess().compile(compileConfig);
  const encodedGraphBytes = new Uint8Array(util.base64.length(encodedGraphInBase64));
  util.base64.decode(encodedGraphInBase64, encodedGraphBytes, 0);
  const compiledGraph = dataform.CompiledGraph.decode(encodedGraphBytes);
  return dataform.CompiledGraph.create({
    ...compiledGraph,
    graphErrors: validate(compiledGraph)
  });
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
        return require.resolve("../vm/compile_loader");
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
      // Handle errors returned by the child process.
      this.childProcess.on("message", (e: Error) => reject(e));

      // Handle UTF-8 string chunks returned by the child process.
      const pipe = this.childProcess.stdio[4];
      const chunks: Buffer[] = [];
      pipe.on("data", (chunk: Buffer) => chunks.push(chunk));
      pipe.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));

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

export const checkDataformJsonValidity = (dataformJsonParsed: { [prop: string]: string }) => {
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
