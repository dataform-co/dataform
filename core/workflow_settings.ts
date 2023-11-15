import * as fs from "fs-extra";
import * as path from "path";
import { load as loadYaml, YAMLException } from "js-yaml";

import { dataform } from "df/protos/ts";

export function readWorkflowSettings(projectDir: string = ""): dataform.ProjectConfig {
  // `dataform.json` is deprecated; new versions of dataform prefer `workflow_settings.yaml`.
  const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
  const dataformJsonPath = path.join(projectDir, "dataform.json");

  // TODO: YAML files can't be required, we instead have to first read it, then convert it to JSON.
  // However, fs isn't available in the sandboxed environment (?).
  const workflowSettingsFileExists = fs.existsSync(workflowSettingsPath);
  const dataformJsonFileExists = fs.existsSync(dataformJsonPath);

  if (workflowSettingsFileExists && dataformJsonFileExists) {
    throw Error(
      "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
    );
  }

  let workflowSettingsAsJson = {};

  if (workflowSettingsFileExists) {
    const workflowSettingsContent = fs.readFileSync(workflowSettingsPath, { encoding: "utf-8" });
    try {
      workflowSettingsAsJson = loadYaml(workflowSettingsContent);
    } catch (e) {
      if (e instanceof YAMLException) {
        throw Error(`workflow_settings.yaml is not a valid YAML file: ${e}`);
      }
    }
    verifyWorkflowSettingsAsJson(workflowSettingsAsJson);
    return dataform.ProjectConfig.create(workflowSettingsAsJson);
  }

  if (dataformJsonFileExists) {
    workflowSettingsAsJson = require(dataformJsonPath);
    verifyWorkflowSettingsAsJson(workflowSettingsAsJson);
    return dataform.ProjectConfig.create(workflowSettingsAsJson);
  }

  throw Error("Failed to resolve workflow_settings.yaml");
}

function verifyWorkflowSettingsAsJson(workflowSettingsAsJson?: object) {
  // TODO(ekrekr): Implement a protobuf field validator. Protobufjs's verify method is not fit for
  // purpose.
  if (!workflowSettingsAsJson) {
    throw Error("workflow_settings.yaml contains invalid fields");
  }
}
