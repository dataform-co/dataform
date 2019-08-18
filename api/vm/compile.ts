import { createGenIndexConfig } from "@dataform/api/vm/gen_index_config";
import * as legacyCompiler from "@dataform/api/vm/legacy_compiler";
import { legacyGenIndex } from "@dataform/api/vm/legacy_gen_index";
import { dataform } from "@dataform/protos";
import * as crypto from "crypto";
import * as fs from "fs";
import * as net from "net";
import * as os from "os";
import * as path from "path";
import { util } from "protobufjs";
import { CompilerFunction, NodeVM } from "vm2";

export function compile(compileConfig: dataform.ICompileConfig): Uint8Array {
  const vmIndexFileName = path.resolve(path.join(compileConfig.projectDir, "index.js"));

  const indexGeneratorVm = new NodeVM({
    wrapper: "none",
    require: {
      context: "sandbox",
      root: compileConfig.projectDir,
      external: true
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
      external: true
    },
    sourceExtensions: ["js", "sql", "sqlx"],
    compiler
  });

  // We return a base64 encoded proto via NodeVM, as returning a Uint8Array directly causes issues.
  const res: string = userCodeVm.run(
    findGenIndex()(createGenIndexConfig(compileConfig)),
    vmIndexFileName
  );
  const encodedGraphBytes = new Uint8Array(util.base64.length(res));
  util.base64.decode(res, encodedGraphBytes, 0);
  return encodedGraphBytes;
}

process.on("message", (compileConfig: dataform.ICompileConfig) => {
  try {
    const compiledResult = compile(compileConfig);
    const outPipe = new net.Socket({ fd: 4 });
    outPipe.write(compiledResult);
  } catch (e) {
    process.send(e);
  }
  process.exit();
});
