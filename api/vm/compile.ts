import * as core from "@dataform/core";
import { dataform } from "@dataform/protos";
import * as crypto from "crypto";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { util } from "protobufjs";
import { NodeVM } from "vm2";
import { createGenIndexConfig } from "./gen_index_config";

export interface ICompileIPCResult {
  path?: string;
  err?: string;
}

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
        indexGeneratorVm.run('return require("@dataform/core").genIndex', vmIndexFileName) ||
        core.genIndex
      );
    } catch (e) {
      return core.genIndex;
    }
  };
  const findCompiler = (): ((code, path) => string) => {
    try {
      return indexGeneratorVm.run(
        'return require("@dataform/core").compilers.compile',
        vmIndexFileName
      );
    } catch (e) {
      return core.compilers.compile;
    }
  };

  const userCodeVm = new NodeVM({
    wrapper: "none",
    require: {
      context: "sandbox",
      root: compileConfig.projectDir,
      external: true
    },
    sourceExtensions: ["js", "sql"],
    compiler: findCompiler()
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
    returnToParent({ path: compileInTmpDir(compileConfig) });
  } catch (e) {
    returnToParent({ err: String(e.stack) });
  }
  process.exit();
});

function compileInTmpDir(compileConfig: dataform.ICompileConfig) {
  // IPC breaks down above 200kb, which is a problem. Instead, pass via file system...
  // TODO: This isn't ideal.
  const tmpDir = path.join(os.tmpdir(), "dataform");
  if (!fs.existsSync(tmpDir)) {
    fs.mkdirSync(tmpDir);
  }
  // Create a consistent hash for the temporary path based on the absolute project path.
  const absProjectPathHash = crypto
    .createHash("md5")
    .update(path.resolve(compileConfig.projectDir))
    .digest("hex");
  const tmpPath = path.join(tmpDir, absProjectPathHash);
  // Clear the transfer path before writing it.
  if (fs.existsSync(tmpPath)) {
    fs.unlinkSync(tmpPath);
  }
  const encodedGraph = compile(compileConfig);
  fs.writeFileSync(tmpPath, encodedGraph);
  // Send back the temp path.
  return tmpPath;
}

function returnToParent(result: ICompileIPCResult) {
  process.send(result);
}
