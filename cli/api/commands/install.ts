import * as fs from "fs";
import { load as loadYaml, YAMLException } from "js-yaml";
import * as path from "path";
import { promisify } from "util";

import * as childProcess from "child_process";
import { dataform } from "df/protos/ts";

export async function install(projectPath: string, skipInstall?: boolean) {
  if (skipInstall) {
    return;
  }
  const resolvedProjectPath = path.resolve(projectPath);
  const workflowSettingsPath = path.join(resolvedProjectPath, "workflow_settings.yaml");
  const packageJsonPath = path.join(resolvedProjectPath, "package.json");
  const packageLockJsonPath = path.join(resolvedProjectPath, "package-lock.json");

  let installCommand = "npm i --ignore-scripts";

  // Core's readWorkflowSettings method cannot be used for this because Core assumes that
  // `require` can read YAML files directly.
  const dataformCoreVersion = readDataformCoreVersionIfPresent(workflowSettingsPath);

  if (dataformCoreVersion) {
    // If there are other packages already in the package.json, specifying a specific package to
    // install will trigger the other packages to be installed too.
    installCommand += ` @dataform/core@${dataformCoreVersion}`;
  }

  if (!dataformCoreVersion && !fs.existsSync(packageJsonPath)) {
    throw new Error(
      "dataformCoreVersion must be specified either in workflow_settings.yaml or via a package.json"
    );
  }

  await promisify(childProcess.exec)(installCommand, { cwd: resolvedProjectPath });
}

function readDataformCoreVersionIfPresent(workflowSettingsPath: string): string {
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
