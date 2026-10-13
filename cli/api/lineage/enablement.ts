/**
 * Lineage emission is enabled by default. Users opt out with
 * `--no-emit-lineage` (per invocation) or `lineage.enabled: false` in
 * workflow_settings.yaml (per project).
 */
export const LINEAGE_ENABLED_BY_DEFAULT = true;

/**
 * Which configuration surface decided whether lineage is emitted.
 *
 * - `cli_flag`: `--emit-lineage` / `--no-emit-lineage` was passed.
 * - `workflow_settings`: workflow_settings.yaml `lineage.enabled` is set.
 * - `default`: neither surface is set; {@link LINEAGE_ENABLED_BY_DEFAULT} applies.
 */
export type LineageEnablementSource = "cli_flag" | "workflow_settings" | "default";

export interface ILineageEnablement {
  enabled: boolean;
  source: LineageEnablementSource;
}

/**
 * Resolves the effective lineage setting. The CLI flag takes precedence over
 * workflow_settings.yaml, which takes precedence over the built-in default.
 * `undefined` means the surface was not set.
 */
export function resolveLineageEnablement(
  cliEmitLineage: boolean | undefined,
  workflowLineageEnabled: boolean | undefined,
): ILineageEnablement {
  if (cliEmitLineage !== undefined) {
    return { enabled: cliEmitLineage, source: "cli_flag" };
  }
  if (workflowLineageEnabled !== undefined) {
    return { enabled: workflowLineageEnabled, source: "workflow_settings" };
  }
  return { enabled: LINEAGE_ENABLED_BY_DEFAULT, source: "default" };
}
