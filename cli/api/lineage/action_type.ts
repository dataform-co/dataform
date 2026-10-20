import { dataform } from "df/protos/ts";

/**
 * Lineage action-type rule shared with Dataform in Google Cloud:
 * every action that runs a BigQuery job emits lineage events. Declarations
 * and other action types that do not run a BigQuery job are excluded.
 *
 * Maps the CLI's execution action type to the value sent in
 * `gcp_bq_pipelines_job.actionType`. Values match the Cloud Dataform
 * `WorkflowExecutionActionInfo.DataformActionType` enum names (`TABLE`,
 * `OPERATIONS`, `ASSERTION` and `PROPERTY_GRAPH`) so both runtimes report
 * the same action type to Knowledge Catalog Lineage. Note that `OPERATIONS`
 * is intentionally plural to match the Cloud Dataform enum.
 */
const LINEAGE_ACTION_TYPES: ReadonlyMap<string, string> = new Map([
  ["table", "TABLE"],
  ["operation", "OPERATIONS"],
  ["assertion", "ASSERTION"],
  ["propertyGraph", "PROPERTY_GRAPH"],
]);

/** Returns true if lineage events are emitted for the given action. */
export function isLineageEligibleAction(action: dataform.IExecutionAction): boolean {
  return toLineageActionType(action) !== undefined;
}

/**
 * Returns the `gcp_bq_pipelines_job.actionType` value for the given action,
 * or undefined if the action type is not lineage-eligible.
 */
export function toLineageActionType(action: dataform.IExecutionAction): string | undefined {
  return LINEAGE_ACTION_TYPES.get(action.type ?? "");
}
