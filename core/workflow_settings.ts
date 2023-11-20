import { dataform } from "df/protos/ts";

export function readWorkflowSettings(projectDir: string): dataform.ProjectConfig {
  const workflowSettingsYaml = maybeRequire(projectDir + "/workflow_settings.yaml");
  // `dataform.json` is deprecated; new versions of Dataform Core prefer `workflow_settings.yaml`.
  const dataformJson = maybeRequire(projectDir + "/dataform.json");

  if (workflowSettingsYaml && dataformJson) {
    throw Error(
      "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
    );
  }

  if (workflowSettingsYaml) {
    const workflowSettingsAsJson = workflowSettingsYaml.asJson();
    verifyWorkflowSettingsAsJson(workflowSettingsAsJson);
    return dataform.ProjectConfig.create(workflowSettingsAsJson);
  }

  if (dataformJson) {
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

function maybeRequire(file: string): any {
  try {
    return require(file);
  } catch (e) {
    if (e instanceof SyntaxError) {
      throw e;
    }
    return undefined;
  }
}
