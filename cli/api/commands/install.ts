import * as fs from "fs";
import { load as loadYaml, YAMLException } from "js-yaml";
import * as path from "path";
import { promisify } from "util";

import * as childProcess from "child_process";
import { dataform } from "df/protos/ts";

export const MISSING_CORE_VERSION_ERROR =
  "dataformCoreVersion must be specified either in workflow_settings.yaml or via a package.json";

export async function install(projectPath: string) {
  const resolvedProjectPath = path.resolve(projectPath);
  const packageJsonPath = path.join(resolvedProjectPath, "package.json");

  // Core's readWorkflowSettings method cannot be used for this because Core assumes that
  // `require` can read YAML files directly.
  const dataformCoreVersion = readDataformCoreVersionFromWorkflowSettings(resolvedProjectPath);
  if (dataformCoreVersion) {
    throw Error(
      "The install method cannot be used when dataformCoreVersion is managed by a " +
        "workflow_settings.yaml file; packages will be installed at compile time instead"
    );
  }

  if (!fs.existsSync(packageJsonPath)) {
    throw new Error(MISSING_CORE_VERSION_ERROR);
  }

  await promisify(childProcess.exec)("npm i --ignore-scripts", { cwd: resolvedProjectPath });
}

export async function runInstallIfWorkflowSettingsDataformCoreVersion(
  projectPath: string
): Promise<string> {
  const resolvedProjectPath = path.resolve(projectPath);
  const packageJsonPath = path.join(resolvedProjectPath, "package.json");
  const packageLockJsonPath = path.join(resolvedProjectPath, "package-lock.json");

  const workflowSettingsDataformCoreVersion = readDataformCoreVersionFromWorkflowSettings(
    resolvedProjectPath
  );
  if (!fs.existsSync(packageJsonPath)) {
    if (!workflowSettingsDataformCoreVersion) {
      throw new Error(MISSING_CORE_VERSION_ERROR);
    }

    fs.writeFileSync(
      packageJsonPath,
      `{
  "dependencies":{
  "@dataform/core": "${workflowSettingsDataformCoreVersion}"
  }
}`
    );
    fs.writeFileSync(
      packageLockJsonPath,
      `{
  "requires": true,
  "dependencies": {
    "@dataform/core": {
      "version": "${workflowSettingsDataformCoreVersion}"
    }
  }
}`
    );
    await promisify(childProcess.exec)("npm i --ignore-scripts", { cwd: resolvedProjectPath });
  }

  return workflowSettingsDataformCoreVersion;
}

export function cleanupNpmFiles(projectPath: string) {
  const resolvedProjectPath = path.resolve(projectPath);

  ["package.json", "package-lock.json"].forEach(filenameToDelete => {
    const pathToDelete = path.join(resolvedProjectPath, filenameToDelete);
    if (fs.existsSync(pathToDelete)) {
      fs.unlinkSync(pathToDelete);
    }
  });

  const nodeModulesPath = path.join(resolvedProjectPath, "node_modules");
  if (fs.existsSync(nodeModulesPath)) {
    fs.rmdirSync(nodeModulesPath);
  }
}

export function readDataformCoreVersionFromWorkflowSettings(resolvedProjectPath: string): string {
  const workflowSettingsPath = path.join(resolvedProjectPath, "workflow_settings.yaml");
  if (!fs.existsSync(workflowSettingsPath)) {
    return "";
  }

  const workflowSettingsContent = fs.readFileSync(workflowSettingsPath, "utf-8");
  let workflowSettingsAsJson = {};
  try {
    workflowSettingsAsJson = loadYaml(workflowSettingsContent);
  } catch (e) {
    if (e instanceof YAMLException) {
      throw Error(`${path} is not a valid YAML file: ${e}`);
    }
    throw e;
  }
  return dataform.WorkflowSettings.create(workflowSettingsAsJson).dataformCoreVersion;
}
