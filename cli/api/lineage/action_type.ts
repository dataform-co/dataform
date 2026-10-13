import { dataform } from "df/protos/ts";

/**
 * Lineage action-type rule shared with the Dataform Backend (DD §4.3):
 * every action that runs a BigQuery job emits lineage events. Declarations
 * and other action types that don't run a BigQuery job are excluded.
 *
 * Maps the CLI's execution action type to the value sent in
 * `gcp_bq_pipelines_job.actionType`. Values mirror the Backend's
 * `WorkflowExecutionActionInfo.DataformActionType` enum names so both
 * runtimes report the same action type to Knowledge Catalog Lineage.
 */
const LINEAGE_ACTION_TYPES: { readonly [executionType: string]: string } = {
  table: "TABLE",
  operation: "OPERATIONS",
  assertion: "ASSERTION",
  propertyGraph: "PROPERTY_GRAPH",
};

/** Returns true if lineage events are emitted for the given action. */
export function isLineageEligibleAction(action: dataform.IExecutionAction): boolean {
  return Object.prototype.hasOwnProperty.call(LINEAGE_ACTION_TYPES, action.type || "");
}

/**
 * Returns the `gcp_bq_pipelines_job.actionType` value for the given action,
 * or undefined if the action type is not lineage-eligible.
 */
export function toLineageActionType(action: dataform.IExecutionAction): string | undefined {
  return isLineageEligibleAction(action) ? LINEAGE_ACTION_TYPES[action.type] : undefined;
}
