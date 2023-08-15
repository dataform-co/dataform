import * as path from "path";
import { CompilerFunction, NodeVM } from "vm2";
import * as net from "net";

import { dataform } from "df/protos/ts";
import { createCoreExecutionRequest, createGenIndexConfig } from "df/sandbox/vm/create_config";
import { decode64 } from "df/common/protos";

function missingValidCorePackageError() {
  return new Error(
    `Could not find a recent installed version of @dataform/core in the project. Ensure packages are installed and upgrade to a recent version.`
  );
}
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
    sourceExtensions: ["js", "sql", "sqlx"],
    compiler
  });

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

export function compileAndSend(socket: string, encodedCompileConfig: string) {
  const client = net.createConnection(socket);
  client.on("connect", () => {
    const compileConfig = decode64(dataform.CompileConfig, encodedCompileConfig);
    try {
      const compiledResult = compile(compileConfig);
      client.write(compiledResult, (err) => {
        client.end(); 
        process.exit(0);
      });
    } catch (e) {
      console.log(e);
      const serializableError = {};
      for (const prop of Object.getOwnPropertyNames(e)) {
        (serializableError as any)[prop] = e[prop];
      }
      client.write(JSON.stringify(serializableError), (err) => {
        client.end(); 
        process.exit(0);
      });
    }
  });
}

if (require.main === module) {
  compileAndSend(process.argv[2], process.argv[3]);
}
