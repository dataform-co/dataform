import {
  IEmitterOptions,
  IStderrLike,
  LineageClientProvider,
  LineageEmitter,
} from "df/cli/api/lineage/emitter";
import { resolveLineageEnablement } from "df/cli/api/lineage/enablement";
import { dataform } from "df/protos/ts";

/**
 * Structured input for {@link createLineageEmitter}. Prefer this over passing
 * yargs argv + executionGraph directly so the factory stays independent of
 * yargs and easy to unit-test.
 */
export interface ILineageEmitterFactoryInput {
  /** Value of --emit-lineage CLI flag. `undefined` means the flag was not passed. */
  cliEmitLineage: boolean | undefined;
  /** Value of workflow_settings.yaml `lineage.enabled`. `undefined` means not set. */
  workflowLineageEnabled: boolean | undefined;
  /** True when the CLI was invoked with --dry-run. */
  dryRun: boolean;
  /** Absolute path of the Dataform project directory (used to derive the workdir hash). */
  projectDir: string;
  /** BigQuery credentials; `undefined` when unavailable. */
  readCredentials: dataform.IBigQuery | undefined;
}

/**
 * Constructs a {@link LineageEmitter} from the effective settings, or returns
 * `undefined` when lineage emission is disabled. Lineage is enabled by default
 * (see {@link resolveLineageEnablement}). When lineage is disabled by an
 * explicit user choice — CLI flag or workflow_settings — a single structured
 * `[lineage] Skipped lineage emission: skip_reason=<label>` line is written to
 * stderr so a troubleshooting user can see WHY nothing was emitted.
 */
export function createLineageEmitter(
  input: ILineageEmitterFactoryInput,
  stderr: IStderrLike = process.stderr,
  clientProvider?: LineageClientProvider,
): LineageEmitter | undefined {
  if (!input.readCredentials) {
    return undefined;
  }

  const enablement = resolveLineageEnablement(input.cliEmitLineage, input.workflowLineageEnabled);

  if (!enablement.enabled) {
    if (enablement.source === "cli_flag") {
      stderr.write(
        "[lineage] Skipped lineage emission: skip_reason=invocation_override (--no-emit-lineage / --emit-lineage=false)\n",
      );
    } else if (enablement.source === "workflow_settings") {
      stderr.write(
        "[lineage] Skipped lineage emission: skip_reason=workflow_opt_out (workflow_settings.yaml lineage.enabled=false)\n",
      );
    }
    return undefined;
  }

  const options: IEmitterOptions = {
    lineageEnabled: true,
    enablementSource: enablement.source,
    dryRun: input.dryRun,
    projectDir: input.projectDir,
  };
  return new LineageEmitter(input.readCredentials, options, clientProvider, stderr);
}
