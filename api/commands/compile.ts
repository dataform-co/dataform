import { ICompileIPCResult } from "@dataform/api/vm/compile";
import { validate } from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { ChildProcess, fork } from "child_process";
import * as fs from "fs";
import * as path from "path";

export async function compile(
  compileConfig: dataform.ICompileConfig
): Promise<dataform.CompiledGraph> {
  // Resolve the path in case it hasn't been resolved already.
  path.resolve(compileConfig.projectDir);
  var compErrors: dataform.CompilationError[] = [];
  try {
    // check dataformJson is valid before we try to compile
    const dataformJson = fs.readFileSync(`${compileConfig.projectDir}/dataform.json`, "utf8");
    var dataformJsonParsed = JSON.parse(dataformJson);
  } catch (e) {
    throw new Error("Compile Error: `dataform.json` is invalid" + e);
  }
  const mandatoryProps = ["warehouse", "defaultSchema"];
  mandatoryProps.forEach(prop => {
    if (!(prop in dataformJsonParsed)) {
      const compileError = dataform.CompilationError.create();
      compileError.message =
        "`dataform.json` does not have mandatory property defined: " + prop + ".";
      compErrors.push(compileError);
    }
  });
  const validDWHs = ["bigquery", "postgres", "redshift", "sqldatawarehouse", "snowflake"];
  if (!!dataformJsonParsed.warehouse && validDWHs.indexOf(dataformJsonParsed.warehouse) === -1) {
    const compileError = dataform.CompilationError.create();
    compileError.message =
      "`dataform.json` has an invalid value on property warehouse: " +
      dataformJsonParsed.warehouse +
      ". Should be one of: " +
      validDWHs;
    compErrors.push(compileError);
  }

  const gErrors: dataform.GraphErrors =
    compErrors !== null
      ? dataform.GraphErrors.create({ compilationErrors: compErrors })
      : dataform.GraphErrors.create({ compilationErrors: [], validationErrors: [] });

  const returnedPath = await CompileChildProcess.forkProcess().compile(compileConfig);
  const contents = fs.readFileSync(returnedPath);
  let compiledGraph = dataform.CompiledGraph.decode(contents);
  fs.unlinkSync(returnedPath);
  const compiledGraphErrors = validate(compiledGraph);
  const joinedErrors = dataform.GraphErrors.create({
    compilationErrors: gErrors.compilationErrors.concat(compiledGraphErrors.compilationErrors),
    validationErrors: gErrors.validationErrors.concat(compiledGraphErrors.validationErrors)
  });

  // Merge graph errors into the compiled graph.
  compiledGraph = dataform.CompiledGraph.create({
    ...compiledGraph,
    graphErrors: joinedErrors
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
