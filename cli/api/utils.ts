import * as fs from "fs-extra";
import { load as loadYaml, YAMLException } from "js-yaml";
import * as path from "path";

import { dataform } from "df/protos/ts";

export function prettyJsonStringify(obj: object) {
  return JSON.stringify(obj, null, 4) + "\n";
}

export function readDataformCoreVersionFromWorkflowSettings(
  resolvedProjectPath: string
): string | undefined {
  const workflowSettingsPath = path.join(resolvedProjectPath, "workflow_settings.yaml");
  if (!fs.existsSync(workflowSettingsPath)) {
    return;
  }

  const workflowSettingsContent = fs.readFileSync(workflowSettingsPath, "utf-8");
  let workflowSettingsAsJson = {};
  try {
    workflowSettingsAsJson = loadYaml(workflowSettingsContent);
  } catch (e) {
    if (e instanceof YAMLException) {
      throw new Error(`${path} is not a valid YAML file: ${e}`);
    }
    throw e;
  }
  return dataform.WorkflowSettings.create(workflowSettingsAsJson).dataformCoreVersion;
}
