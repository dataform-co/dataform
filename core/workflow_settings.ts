import { verifyObjectMatchesProto } from "df/common/protos";
import { version } from "df/core/version";
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
    const workflowSettingsAsJson = workflowSettingsYaml.asJson;
    if (!workflowSettingsAsJson) {
      throw Error("workflow_settings.yaml is invalid");
    }
    return workflowSettingsAsProjectConfig(verifyWorkflowSettingsAsJson(workflowSettingsAsJson));
  }

  if (dataformJson) {
    // Dataform JSON used the compiled graph's config proto, rather than workflow settings.
    try {
      return dataform.ProjectConfig.create(
        verifyObjectMatchesProto(dataform.ProjectConfig, dataformJson)
      );
    } catch (e) {
      if (e instanceof ReferenceError) {
        throw ReferenceError(`Dataform json error: ${e.message}`);
      }
      throw e;
    }
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

  // The caller of Dataform Core should ensure that the correct version is installed.
  if (!!workflowSettings.dataformCoreVersion && workflowSettings.dataformCoreVersion !== version) {
    throw Error(
      `Version mismatch: workflow settings specifies version ${workflowSettings.dataformCoreVersion}` +
        `, but ${version} was found`
    );
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

export function workflowSettingsAsProjectConfig(
  workflowSettings: dataform.WorkflowSettings
): dataform.ProjectConfig {
  const projectConfig = dataform.ProjectConfig.create();
  if (workflowSettings.defaultProject) {
    projectConfig.defaultDatabase = workflowSettings.defaultProject;
  }
  if (workflowSettings.defaultDataset) {
    projectConfig.defaultSchema = workflowSettings.defaultDataset;
  }
  if (workflowSettings.defaultLocation) {
    projectConfig.defaultLocation = workflowSettings.defaultLocation;
  }
  if (workflowSettings.defaultAssertionDataset) {
    projectConfig.assertionSchema = workflowSettings.defaultAssertionDataset;
  }
  if (workflowSettings.vars) {
    projectConfig.vars = workflowSettings.vars;
  }
  if (workflowSettings.projectSuffix) {
    projectConfig.databaseSuffix = workflowSettings.projectSuffix;
  }
  if (workflowSettings.datasetSuffix) {
    projectConfig.schemaSuffix = workflowSettings.datasetSuffix;
  }
  if (workflowSettings.namePrefix) {
    projectConfig.tablePrefix = workflowSettings.namePrefix;
  }
  if (workflowSettings.defaultNotebookRuntimeOptions) {
    projectConfig.defaultNotebookRuntimeOptions = {};
    if (workflowSettings.defaultNotebookRuntimeOptions.outputBucket) {
      projectConfig.defaultNotebookRuntimeOptions.outputBucket =
        workflowSettings.defaultNotebookRuntimeOptions.outputBucket;
    }
  }
  projectConfig.warehouse = "bigquery";
  return projectConfig;
}
