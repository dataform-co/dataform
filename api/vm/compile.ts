import * as net from "net";
import * as path from "path";
import { promisify } from "util";
import { CompilerFunction, NodeVM } from "vm2";

import { createGenIndexConfig } from "df/api/vm/gen_index_config";
import * as legacyCompiler from "df/api/vm/legacy_compiler";
import { legacyGenIndex } from "df/api/vm/legacy_gen_index";
import { dataform } from "df/protos/ts";

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
  return userCodeVm.run(genIndex(createGenIndexConfig(compileConfig)), vmIndexFileName);
}

export function listenForCompileRequest() {
  process.on("message", (compileConfig: dataform.ICompileConfig) => {
    const handleError = (e: any) => {
      const serializableError = {};
      for (const prop of Object.getOwnPropertyNames(e)) {
        (serializableError as any)[prop] = e[prop];
      }
      process.send(serializableError);
    }
    try {
      const compiledResult = compile(compileConfig);
      const writer = new net.Socket({ fd: 4 });
      writer.write(compiledResult, (err) => {
        if (err) {
          handleError(err);
        }
        process.exit();
      });
    } catch (e) {
      handleError(e);
      process.exit()
    }
  });
}

if (require.main === module) {
  listenForCompileRequest();
}
