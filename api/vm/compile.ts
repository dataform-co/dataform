import * as core from "@dataform/core";
import * as crypto from "crypto";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { util } from "protobufjs";
import { NodeVM } from "vm2";
import { genIndex } from "../gen_index";

export interface ICompileIPCParameters {
  projectDir: string;
  schemaSuffix?: string;
}

export interface ICompileIPCResult {
  path?: string;
  err?: string;
}

export function compile(
  projectDir: string,
  schemaSuffix?: string,
): Uint8Array {
  const vm = new NodeVM({
    wrapper: "none",
    require: {
      context: "sandbox",
      root: projectDir,
      external: true
    },
    sourceExtensions: ["js", "sql"],
    compiler: (code, path) => core.compilers.compile(code, path)
  });

  const indexScript = genIndex(projectDir, "", schemaSuffix);
  // We return a base64 encoded proto via NodeVM, as returning a Uint8Array directly causes issues.
  const res: string = vm.run(indexScript, path.resolve(path.join(projectDir, "index.js")));
  const encodedGraphBytes = new Uint8Array(util.base64.length(res));
  util.base64.decode(res, encodedGraphBytes, 0);
  return encodedGraphBytes;
}

process.on("message", (compileIpcParameters: ICompileIPCParameters) => {
  try {
    returnToParent({ path: compileInTmpDir(compileIpcParameters) });
  } catch (e) {
    returnToParent({ err: String(e) });
  }
  process.exit();
});

function compileInTmpDir(compileIpcParameters: ICompileIPCParameters) {
  // IPC breaks down above 200kb, which is a problem. Instead, pass via file system...
  // TODO: This isn't ideal.
  const tmpDir = path.join(os.tmpdir(), "dataform");
  if (!fs.existsSync(tmpDir)) {
    fs.mkdirSync(tmpDir);
  }
  // Create a consistent hash for the temporary path based on the absolute project path.
  const absProjectPathHash = crypto
    .createHash("md5")
    .update(path.resolve(compileIpcParameters.projectDir))
    .digest("hex");
  const tmpPath = path.join(tmpDir, absProjectPathHash);
  // Clear the transfer path before writing it.
  if (fs.existsSync(tmpPath)) {
    fs.unlinkSync(tmpPath);
  }
  const encodedGraph = compile(
    compileIpcParameters.projectDir,
    compileIpcParameters.schemaSuffix
  );
  fs.writeFileSync(tmpPath, encodedGraph);
  // Send back the temp path.
  return tmpPath;
}

function returnToParent(result: ICompileIPCResult) {
  process.send(result);
}
