import * as os from "os";

import { ChildProcess } from "child_process";

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
    if (os.arch() === 'arm64') {
      return "nodejs_linux_arm64";
    } else {
      return "nodejs_linux_amd64";
    }
  }
}

// Note: it would be more correct for these to be injected by blaze at run time.
export const nodePath = `external/${platformPath()}/bin/node`;
export const npmPath = `external/${platformPath()}/bin/npm`;
export const corePackageTarPath = "packages/@dataform/core/package.tar.gz";

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
