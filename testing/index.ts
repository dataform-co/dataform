import { ChildProcess } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

export * from "df/testing/hook";
export * from "df/testing/suite";
export * from "df/testing/test";
export * from "df/testing/runner";

export const platformPath = () => {
  if (os.platform() === "darwin") {
    if (os.arch() === "arm64") {
      return "nodejs_darwin_arm64";
    } else {
      return "nodejs_darwin_amd64";
    }
  } else {
    if (os.arch() === "arm64") {
      return "nodejs_linux_arm64";
    } else {
      return "nodejs_linux_amd64";
    }
  }
};

const runfilesDir = process.env.RUNFILES || "";
let workspaceName = "df";
if (runfilesDir && !fileExists(path.resolve(runfilesDir, "df"))) {
  workspaceName = "_main";
}

function fileExists(filePath: string): boolean {
  try {
    fs.accessSync(filePath);
    return true;
  } catch {
    return false;
  }
}

const getBzlmodNpmPath = () => {
  if (!runfilesDir) {
    return "";
  }
  const canonicalRepoName = `_main~node_ext~${platformPath()}`;
  const bzlmodPath = path.resolve(runfilesDir, canonicalRepoName, "bin/nodejs/bin/npm");
  if (fileExists(bzlmodPath)) {
    return bzlmodPath;
  }
  const legacyPath = path.resolve(runfilesDir, platformPath(), "bin/npm");
  if (fileExists(legacyPath)) {
    return legacyPath;
  }
  return "";
};

const getBzlmodNodePath = () => {
  if (!runfilesDir) {
    return process.execPath;
  }
  const canonicalRepoName = `_main~node_ext~${platformPath()}`;
  const bzlmodPath = path.resolve(runfilesDir, canonicalRepoName, "bin/nodejs/bin/node");
  if (fileExists(bzlmodPath)) {
    return bzlmodPath;
  }
  const legacyPath = path.resolve(runfilesDir, platformPath(), "bin/node");
  if (fileExists(legacyPath)) {
    return legacyPath;
  }
  return process.execPath;
};

export const nodePath = getBzlmodNodePath();
const bzlmodNpm = getBzlmodNpmPath();
export const npmPath = bzlmodNpm || "npm";

if (npmPath !== "npm") {
  process.env.PATH = `${path.dirname(npmPath)}:${process.env.PATH}`;
}

export const corePackageTarPath = runfilesDir
  ? path.resolve(runfilesDir, workspaceName, "packages/@dataform/core/package.tar.gz")
  : "packages/@dataform/core/package.tar.gz";

export async function getProcessResult(childProcess: ChildProcess) {
  let stderr = "";
  childProcess.stderr.pipe(process.stderr);
  childProcess.stderr.on("data", chunk => (stderr += String(chunk)));
  let stdout = "";
  childProcess.stdout.pipe(process.stdout);
  childProcess.stdout.on("data", chunk => (stdout += String(chunk)));
  const exitCode: number = await new Promise(resolve => {
    childProcess.on("close", resolve);
  });
  return { exitCode, stdout, stderr };
}

export function asPlainObject<T>(object: T): T {
  return JSON.parse(JSON.stringify(object)) as T;
}

export function cleanSql(value: string) {
  let cleanValue = value;
  while (true) {
    const newCleanVal = cleanValue
      .replace("  ", " ")
      .replace("\t", " ")
      .replace("\n", " ")
      .replace("( ", "(")
      .replace(" )", ")");
    if (newCleanVal !== cleanValue) {
      cleanValue = newCleanVal;
      continue;
    }
    return newCleanVal.toLowerCase().trim();
  }
}
