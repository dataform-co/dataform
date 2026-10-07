import { ChildProcess } from "child_process";
import * as fs from "fs-extra";
import { dump as dumpYaml } from "js-yaml";
import * as path from "path";

import { dataform } from "df/protos/ts";

export * from "df/testing/hook";
export * from "df/testing/suite";
export * from "df/testing/test";
export * from "df/testing/runner";

export const nodePath = path.resolve(process.env.JS_BINARY__NODE_BINARY || process.execPath);
export const npmPath = path.join(path.dirname(nodePath), "npm");
process.env.PATH = `${path.dirname(nodePath)}:${process.env.PATH}`;
export const corePackageTarPath = path.resolve("packages/@dataform/core/package.tar.gz");

export async function getProcessResult(childProcess: ChildProcess) {
  let stderr = "";
  childProcess.stderr.pipe(process.stderr);
  childProcess.stderr.on("data", (chunk) => (stderr += String(chunk)));
  let stdout = "";
  childProcess.stdout.pipe(process.stdout);
  childProcess.stdout.on("data", (chunk) => (stdout += String(chunk)));
  const exitCode: number = await new Promise((resolve) => {
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

export function writeDefinitionFile(projectDir: string, filename: string, content: string): void {
  const fullPath = path.join(projectDir, "definitions", filename);
  fs.ensureFileSync(fullPath);
  fs.writeFileSync(fullPath, content);
}

export function writeWorkflowSettingsFile(
  projectDir: string,
  settings: string | dataform.IWorkflowSettings,
): void {
  const fullPath = path.join(projectDir, "workflow_settings.yaml");
  const content = typeof settings === "string" ? settings : dumpYaml(settings);
  fs.ensureFileSync(fullPath);
  fs.writeFileSync(fullPath, content);
}
