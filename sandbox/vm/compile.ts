import * as path from "path";
import { CompilerFunction, NodeVM } from "vm2";
import { loadPyodide } from "pyodide";

import { dataform } from "df/protos/ts";
import { createCoreExecutionRequest, createGenIndexConfig } from "df/sandbox/vm/create_config";

function missingValidCorePackageError() {
  return new Error(
    `Could not find a recent installed version of @dataform/core in the project. Ensure packages are installed and upgrade to a recent version.`
  );
}
export async function compile(compileConfig: dataform.ICompileConfig) {
  const vmIndexFileName = path.resolve(path.join(compileConfig.projectDir, "index.js"));

  const indexGeneratorVm = new NodeVM({
    wrapper: "none",
    require: {
      context: "sandbox",
      root: compileConfig.projectDir,
      external: true,
      builtin: ["path"]
    }
  });

  const findCompiler = (): CompilerFunction => {
    try {
      return indexGeneratorVm.run('return require("@dataform/core").compiler', vmIndexFileName);
    } catch (e) {
      throw missingValidCorePackageError();
    }
  };
  const compiler = findCompiler();
  if (!compiler) {
    throw missingValidCorePackageError();
  }

  const userCodeVm = new NodeVM({
    wrapper: "none",
    require: {
      builtin: ["path"],
      context: "sandbox",
      external: true,
      root: compileConfig.projectDir,
      resolve: (moduleName, parentDirName) =>
        path.join(parentDirName, path.relative(parentDirName, compileConfig.projectDir), moduleName)
    },
    sourceExtensions: ["js", "sql", "sqlx", "py"],
    compiler
  });

  // Need to take care here. Depending on what globals etc are now available in the javascript `pyodide` object,
  // this may offer a way to run unsandboxed code. In particular, `jsglobals`.
  // Shouldn't be a problem in closed-source, where this can happen inside a sandbox.
  const pyodide = await loadPyodide();
  userCodeVm.setGlobal("pyodide", pyodide);

  if (compileConfig.useMain) {
    try {
    return userCodeVm.run(
      `return require("@dataform/core").main("${createCoreExecutionRequest(compileConfig)}")`,
      vmIndexFileName
    );
    } catch (e) {
      throw missingValidCorePackageError();
    }
  }

  // Generate an index file and run it.
  const findGenIndex = (): ((base64EncodedConfig: string) => string) => {
    try {
      return indexGeneratorVm.run(
        'return require("@dataform/core").indexFileGenerator',
        vmIndexFileName
      );
    } catch (e) {
      throw missingValidCorePackageError();
    }
  };
  const genIndex = findGenIndex();

  return userCodeVm.run(genIndex(createGenIndexConfig(compileConfig)), vmIndexFileName);
}

export function listenForCompileRequest() {
  process.on("message", (compileConfig: dataform.ICompileConfig) => {
    compile(compileConfig)
      .then(compiledResult => process.send(compiledResult))
      .catch(e => {
        const serializableError = {};
        for (const prop of Object.getOwnPropertyNames(e)) {
          (serializableError as any)[prop] = e[prop];
        }
        process.send(serializableError);
      });
  });
}

if (require.main === module) {
  listenForCompileRequest();
}
