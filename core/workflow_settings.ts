// `dataform.json` is deprecated; new versions of dataform prefer `workflow_settings.yaml`.
const WORKFLOW_SETTINGS_FILENAME = "workflow_settings.yaml";
const DATAFORM_JSON_FILENAME = "dataform.json";

export function getWorkflowSettings() {
  const workflowSettingsRaw = require(WORKFLOW_SETTINGS_FILENAME);
  console.log("WORKFLOW SETTINGS RAW FILE:", workflowSettingsRaw);
}
