import { createGenIndexConfig } from "df/api/vm/gen_index_config";
import * as legacyCompiler from "df/api/vm/legacy_compiler";
import { legacyGenIndex } from "df/api/vm/legacy_gen_index";
import { dataform } from "df/protos";
import * as fs from "fs";
import * as path from "path";
import { CompilerFunction, NodeVM } from "vm2";

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
      context: "sandbox",
      root: compileConfig.projectDir,
      external: true,
      builtin: ["path"]
    },
    sourceExtensions: ["js", "sql", "sqlx"],
    compiler
  });

  const res: string = userCodeVm.run(
    genIndex(createGenIndexConfig(compileConfig)),
    vmIndexFileName
  );
  return res;
}

export function listenForCompileRequest() {
  process.on("message", (compileConfig: dataform.ICompileConfig) => {
    try {
      const compiledResult = compile(compileConfig);
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      const writeable = fs.createWriteStream(null, { fd: 4 });
      writeable.write(compiledResult, "utf8");
    } catch (e) {
      process.send(e);
    }
    process.exit();
  });
}

if (require.main === module) {
  listenForCompileRequest();
}
