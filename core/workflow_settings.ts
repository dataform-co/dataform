import { dataform } from "df/protos/ts";

export function readWorkflowSettings(projectDir: string): dataform.ProjectConfig {
  let workflowSettingsFileExists = false;
  let workflowSettingsPath: string | undefined;
  try {
    workflowSettingsPath = require.resolve(projectDir + "/workflow_settings.yaml");
    workflowSettingsFileExists = true;
  } catch (e) {}

  // `dataform.json` is deprecated; new versions of Dataform Core prefer `workflow_settings.yaml`.
  let dataformJsonFileExists = false;
  let dataformJsonPath: string | undefined;
  try {
    dataformJsonPath = require.resolve(projectDir + "/dataform.json");
    dataformJsonFileExists = true;
  } catch (e) {}

  if (workflowSettingsFileExists && dataformJsonFileExists) {
    throw Error(
      "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
    );
  }

  if (workflowSettingsFileExists) {
    const workflowSettingsAsJson = require(workflowSettingsPath).asJson();
    verifyWorkflowSettingsAsJson(workflowSettingsAsJson);
    return dataform.ProjectConfig.create(workflowSettingsAsJson);
  }

  if (dataformJsonFileExists) {
    const dataformJson = require(dataformJsonPath);
    verifyWorkflowSettingsAsJson(dataformJson);
    return dataform.ProjectConfig.create(dataformJson);
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
