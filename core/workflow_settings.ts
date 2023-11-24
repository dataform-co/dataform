import { verifyObjectMatchesProto } from "df/common/protos";
import { dataform } from "df/protos/ts";

declare var __webpack_require__: any;
declare var __non_webpack_require__: any;
const nativeRequire = typeof __webpack_require__ === "function" ? __non_webpack_require__ : require;

export function readWorkflowSettings(): dataform.ProjectConfig {
  const workflowSettingsYaml = maybeRequire("workflow_settings.yaml");
  // `dataform.json` is deprecated; new versions of Dataform Core prefer `workflow_settings.yaml`.
  const dataformJson = maybeRequire("dataform.json");

  if (workflowSettingsYaml && dataformJson) {
    throw Error(
      "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
    );
  }

  if (workflowSettingsYaml) {
    const workflowSettingsAsJson = workflowSettingsYaml.asJson();
    if (!workflowSettingsAsJson) {
      throw Error("workflow_settings.yaml is invalid");
    }
    verifyWorkflowSettingsAsJson(workflowSettingsAsJson);
    return dataform.ProjectConfig.fromObject(workflowSettingsAsJson);
  }

  if (dataformJson) {
    verifyWorkflowSettingsAsJson(dataformJson);
    return dataform.ProjectConfig.fromObject(dataformJson);
  }

  throw Error("Failed to resolve workflow_settings.yaml");
}

function verifyWorkflowSettingsAsJson(workflowSettingsAsJson: object) {
  try {
    verifyObjectMatchesProto(dataform.ProjectConfig, workflowSettingsAsJson);
  } catch (e) {
    if (e instanceof ReferenceError) {
      throw ReferenceError(`Workflow settings error: ${e.message}`);
    }
    throw e;
  }
}

function maybeRequire(file: string): any {
  try {
    // tslint:disable-next-line: tsr-detect-non-literal-require
    return nativeRequire(file);
  } catch (e) {
    if (e instanceof SyntaxError) {
      throw e;
    }
    return undefined;
  }
}
