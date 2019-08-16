import { ICompileIPCResult } from "@dataform/api/vm/compile";
import { validate } from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { ChildProcess, fork } from "child_process";
import * as fs from "fs";
import * as path from "path";

const validDatawarehouses = ["bigquery", "postgres", "redshift", "sqldatawarehouse", "snowflake"];
const mandatoryProps: Array<keyof dataform.IProjectConfig> = ["warehouse", "defaultSchema"];
const simpleCheckProps: Array<keyof dataform.IProjectConfig> = [
  "assertionSchema",
  "schemaSuffix",
  "gcloudProjectId",
  "defaultSchema"
];

export async function compile(
  compileConfig: dataform.ICompileConfig
): Promise<dataform.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  path.resolve(compileConfig.projectDir);

  try {
    // check dataformJson is valid before we try to compile
    const dataformJson = fs.readFileSync(`${compileConfig.projectDir}/dataform.json`, "utf8");
    const checkValidity = checkDataformJsonValidity(JSON.parse(dataformJson));
    if (checkValidity) {
      throw checkValidity;
    }
  } catch (e) {
    throw new Error(`Compile Error: 'dataform.json' is invalid. ${e}`);
  }

  const returnedPath = await CompileChildProcess.forkProcess().compile(compileConfig);
  const contents = fs.readFileSync(returnedPath);
  let compiledGraph = dataform.CompiledGraph.decode(contents);
  fs.unlinkSync(returnedPath);

  // Merge graph errors into the compiled graph.
  compiledGraph = dataform.CompiledGraph.create({
    ...compiledGraph,
    graphErrors: validate(compiledGraph)
  });
  return compiledGraph;
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

  public async compile(compileConfig: dataform.ICompileConfig) {
    const compileInChildProcess = new Promise<string>(async (resolve, reject) => {
      this.childProcess.on("message", (result: ICompileIPCResult) => {
        if (result.err) {
          reject(new Error(result.err));
        } else {
          // We receive back a path where the compiled graph was written in proto format.
          resolve(result.path);
        }
      });
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

function checkDataformJsonValidity(dataformJsonParsed: { [prop: string]: string }): string {
  const invalidDatawarehouseProp = function(): string {
    return dataformJsonParsed.warehouse &&
      !validDatawarehouses.includes(dataformJsonParsed.warehouse)
      ? `Invalid value on property warehouse: ${
          dataformJsonParsed.warehouse
        }. Should be one of: ${validDatawarehouses.join(", ")}.`
      : null;
  };
  const invalidProp = function(): string {
    const invProp = simpleCheckProps.find(prop => {
      return prop in dataformJsonParsed && dataformJsonParsed[prop].length === 0;
    });
    return invProp ? `Invalid value on property ${invProp}. Should not be blank.` : null;
  };
  const missingMandatoryProp = function(): string {
    const missMandatoryProp = mandatoryProps.find(prop => {
      return !(prop in dataformJsonParsed);
    });
    return missMandatoryProp ? `Missing mandatory property: ${missMandatoryProp}.` : null;
  };
  return invalidDatawarehouseProp() || invalidProp() || missingMandatoryProp();
}
