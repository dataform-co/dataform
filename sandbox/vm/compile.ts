import * as path from "path";
import { CompilerFunction, NodeVM } from "vm2";

import { dataform } from "df/protos/ts";
import { createGenIndexConfig } from "df/sandbox/vm/gen_index_config";
import * as legacyCompiler from "df/sandbox/vm/legacy_compiler";
import { legacyGenIndex } from "df/sandbox/vm/legacy_gen_index";

export function compile(compileConfig: dataform.ICompileConfig) {
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
      return (
        indexGeneratorVm.run('return require("@dataform/core").compiler', vmIndexFileName) ||
        legacyCompiler.compile
      );
    } catch (e) {
      return legacyCompiler.compile;
    }
  };
  const compiler = findCompiler();
  if (!compiler) {
    throw new Error("Could not find compiler function.");
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
    sourceExtensions: ["js", "sql", "sqlx"],
    compiler
  });

  let mainExists: boolean;
  try {
    mainExists = !!userCodeVm.run(
      `return require("@dataform/core").main`,
      vmIndexFileName
    );
  } catch (e) {
    // This is OK, main may not exist on older @dataform/core versions.
  }

  if (mainExists) {
    return userCodeVm.run(
      `return require("@dataform/core").main("${createGenIndexConfig(compileConfig)}")`,
      vmIndexFileName
    );
  }

  // No main exists, generate an index file and run it.

  // TODO: Once all users of @dataform/core are updated to include compiler functions, remove
  // this exception handling code (and assume existence of genIndex / compiler functions in @dataform/core).
  const findGenIndex = (): ((base64EncodedConfig: string) => string) => {
    try {
      return (
        indexGeneratorVm.run(
          'return require("@dataform/core").indexFileGenerator',
          vmIndexFileName
        ) || legacyGenIndex
      );
    } catch (e) {
      return legacyGenIndex;
    }
  };
  const genIndex = findGenIndex();

  return userCodeVm.run(genIndex(createGenIndexConfig(compileConfig)), vmIndexFileName);
}

export function listenForCompileRequest() {
  process.on("message", (compileConfig: dataform.ICompileConfig) => {
    try {
      const compiledResult = compile(compileConfig);
      process.send(compiledResult);
    } catch (e) {
      const serializableError = {};
      for (const prop of Object.getOwnPropertyNames(e)) {
        (serializableError as any)[prop] = e[prop];
      }
      process.send(serializableError);
    }
  });
}

if (require.main === module) {
  listenForCompileRequest();
}
