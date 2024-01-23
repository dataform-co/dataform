import { verifyObjectMatchesProto } from "df/common/protos";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

declare var __webpack_require__: any;
declare var __non_webpack_require__: any;
const nativeRequire = typeof __webpack_require__ === "function" ? __non_webpack_require__ : require;

export function readWorkflowSettings(): dataform.WorkflowSettings {
  const workflowSettingsYaml = maybeRequire("workflow_settings.yaml");
  // `dataform.json` is deprecated; new versions of Dataform Core prefer `workflow_settings.yaml`.
  const dataformJson = maybeRequire("dataform.json");

  if (workflowSettingsYaml && dataformJson) {
    throw Error(
      "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
    );
  }

  if (workflowSettingsYaml) {
    const workflowSettingsAsJson = workflowSettingsYaml.asJson;
    if (!workflowSettingsAsJson) {
      throw Error("workflow_settings.yaml is invalid");
    }
    return verifyWorkflowSettingsAsJson(workflowSettingsAsJson);
  }

  if (dataformJson) {
    return verifyWorkflowSettingsAsJson(dataformJson);
  }

  throw Error("Failed to resolve workflow_settings.yaml");
}

function verifyWorkflowSettingsAsJson(workflowSettingsAsJson: object): dataform.WorkflowSettings {
  let workflowSettings = dataform.WorkflowSettings.create();
  try {
    workflowSettings = dataform.WorkflowSettings.create(
      verifyObjectMatchesProto(
        dataform.WorkflowSettings,
        workflowSettingsAsJson as {
          [key: string]: any;
        }
      )
    );
  } catch (e) {
    if (e instanceof ReferenceError) {
      throw ReferenceError(`Workflow settings error: ${e.message}`);
    }
    throw e;
  }
  // tslint:disable-next-line: no-string-literal
  if (!workflowSettings.warehouse) {
    // The warehouse field still set, though deprecated, to simplify compatability with dependents.
    // tslint:disable-next-line: no-string-literal
    workflowSettings.warehouse = "bigquery";
  }

  // The caller of Dataform Core should ensure that the correct version is installed.
  if (!!workflowSettings.dataformCoreVersion && workflowSettings.dataformCoreVersion !== version) {
    throw Error(
      `Version mismatch: workflow settings specifies version ${workflowSettings.dataformCoreVersion}` +
        `, but ${version} was found`
    );
  }

  // tslint:disable-next-line: no-string-literal
  if (workflowSettings.warehouse !== "bigquery") {
    throw Error("Workflow settings error: the warehouse field is deprecated");
  }
  return workflowSettings;
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
