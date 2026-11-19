import * as fs from "fs-extra";
import { load as loadYaml, YAMLException } from "js-yaml";
import * as path from "path";
import * as semver from "semver";
import * as util from "util";

import { dataform } from "df/protos/ts";

export function prettyJsonStringify(obj: object) {
  return JSON.stringify(obj, null, 4) + "\n";
}

function formatInvalidCoreVersion(coreVersion: unknown): string {
  if (typeof coreVersion === "string") {
    return JSON.stringify(coreVersion);
  }
  if (coreVersion === null || typeof coreVersion !== "object") {
    return String(coreVersion);
  }
  try {
    return JSON.stringify(coreVersion) ?? util.inspect(coreVersion, { breakLength: Infinity });
  } catch {
    return util.inspect(coreVersion, { breakLength: Infinity });
  }
}

export function validateDataformCoreVersion(coreVersion: unknown): void {
  if (typeof coreVersion !== "string" || semver.valid(coreVersion) !== coreVersion) {
    throw new Error(
      `Invalid dataformCoreVersion ${formatInvalidCoreVersion(coreVersion)}: must be a valid semantic version.`,
    );
  }
}

export function readDataformCoreVersionFromWorkflowSettings(
  resolvedProjectPath: string,
): string | undefined {
  return readConfigFromWorkflowSettings(resolvedProjectPath)?.dataformCoreVersion;
}

export function readConfigFromWorkflowSettings(
  resolvedProjectPath: string,
): dataform.WorkflowSettings | undefined {
  const workflowSettingsPath = path.join(resolvedProjectPath, "workflow_settings.yaml");
  if (!fs.existsSync(workflowSettingsPath)) {
    return;
  }

  const workflowSettingsContent = fs.readFileSync(workflowSettingsPath, "utf-8");
  let workflowSettingsAsJson: unknown = {};
  try {
    workflowSettingsAsJson = loadYaml(workflowSettingsContent);
  } catch (e) {
    if (e instanceof YAMLException) {
      throw new Error(`${workflowSettingsPath} is not a valid YAML file: ${e}`);
    }
    throw e;
  }
  if (
    workflowSettingsAsJson !== null &&
    typeof workflowSettingsAsJson === "object" &&
    !Array.isArray(workflowSettingsAsJson)
  ) {
    const rawCoreVersion = (workflowSettingsAsJson as { dataformCoreVersion?: unknown })
      .dataformCoreVersion;
    if (rawCoreVersion !== undefined && rawCoreVersion !== null && rawCoreVersion !== "") {
      validateDataformCoreVersion(rawCoreVersion);
    }
    return dataform.WorkflowSettings.create(workflowSettingsAsJson as dataform.IWorkflowSettings);
  }
  return dataform.WorkflowSettings.create({});
}
