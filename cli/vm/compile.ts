import * as fs from "fs";
import * as glob from "glob";
import * as path from "path";
import * as semver from "semver";
import { CompilerFunction, NodeVM } from "vm2";

import { encode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";

export function compile(compileConfig: dataform.ICompileConfig) {
  if (
    !fs.existsSync(
      path.join(compileConfig.projectDir, "node_modules", "@dataform", "core", "bundle.js")
    )
  ) {
    throw new Error(
      "Could not find a recent installed version of @dataform/core in the project. Check that " +
        "either `dataformCoreVersion` is specified in `workflow_settings.yaml`, or " +
        "`@dataform/core` is specified in `package.json`. If using `package.json`, then run " +
        "`dataform install`."
    );
  }
  const vmIndexFileName = path.resolve(path.join(compileConfig.projectDir, "index.js"));

  // First retrieve a compiler function for vm2 to process files.
  const indexGeneratorVm = new NodeVM({
    wrapper: "none",
    require: {
      context: "sandbox",
      root: compileConfig.projectDir,
      external: true,
      builtin: ["path"]
    }
  });
  const compiler: CompilerFunction = indexGeneratorVm.run(
    'return require("@dataform/core").compiler',
    vmIndexFileName
  );

  // Then use vm2's native compiler integration to apply the compiler to files.
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
    sourceExtensions: ["js", "sql", "sqlx", "yaml"],
    compiler
  });

  const dataformCoreVersion: string = userCodeVm.run(
    'return require("@dataform/core").version || "0.0.0"',
    vmIndexFileName
  );
  if (semver.lt(dataformCoreVersion, "3.0.0-alpha.0")) {
    throw new Error("@dataform/core ^3.0.0 required.");
  }

  return userCodeVm.run(
    `return require("@dataform/core").main("${createCoreExecutionRequest(compileConfig)}")`,
    vmIndexFileName
  );
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

/**
 * @returns a base64 encoded {@see dataform.CoreExecutionRequest} proto.
 */
function createCoreExecutionRequest(compileConfig: dataform.ICompileConfig): string {
  const filePaths = Array.from(
    new Set<string>(glob.sync("!(node_modules)/**/*.*", { cwd: compileConfig.projectDir }))
  );

  return encode64(dataform.CoreExecutionRequest, {
    // Add the list of file paths to the compile config if not already set.
    compile: { compileConfig: { filePaths, ...compileConfig } }
  });
}
