import {
  IEmitterOptions,
  IStderrLike,
  LineageClientProvider,
  LineageEmitter,
} from "df/cli/api/lineage/emitter";
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
  /** Lineage location */
  location: string | undefined;
}

/**
 * Constructs a {@link LineageEmitter} from the effective settings, or returns
 * `undefined` when lineage emission is disabled. When lineage is disabled by an
 * explicit user choice — CLI flag or workflow_settings — a single structured
 * `[lineage] Skipped lineage emission: skip_reason=<label>` line is written to
 * stderr so a troubleshooting user can see WHY nothing was emitted. When
 * neither surface has been touched, we stay silent (opt-in is the default).
 *
 * Cloud Data Lineage is not available outside the Google Default Universe
 * (e.g. GDC / TPC). If credentials set a `universeDomain` other than
 * `googleapis.com`, emission is skipped with `skip_reason=unsupported_universe`
 * even when lineage is otherwise enabled. The default domain is not a reason
 * to skip: GDU service-account keys include it.
 */
export function createLineageEmitter(
  input: ILineageEmitterFactoryInput,
  stderr: IStderrLike = process.stderr,
  clientProvider?: LineageClientProvider,
): LineageEmitter | undefined {
  if (!input.readCredentials) {
    return undefined;
  }

  const lineageEnabled = input.cliEmitLineage ?? input.workflowLineageEnabled ?? false;

  if (!lineageEnabled) {
    if (input.cliEmitLineage === false) {
      stderr.write(
        "[lineage] Skipped lineage emission: skip_reason=invocation_override (--emit-lineage=false)\n",
      );
    } else if (input.workflowLineageEnabled === false) {
      stderr.write(
        "[lineage] Skipped lineage emission: skip_reason=workflow_opt_out (workflow_settings.yaml lineage.enabled=false)\n",
      );
    }
    return undefined;
  }

  if (!input.location && !input.readCredentials.location) {
    stderr.write(
      `Warning: Lineage emission is enabled, but no location was specified. Lineage requires a ` +
        `specific location, set either as "defaultLocation" in workflow_settings.yaml or as ` +
        `"location" in .df-credentials.json. Lineage will not be emitted.\n`,
    );
    return undefined;
  }

  // googleapis.com is the GDU default, including on service-account keys created today.
  // Only a different universe means the Lineage API is unavailable.
  const universeDomain = input.readCredentials.universeDomain;
  if (universeDomain && universeDomain !== "googleapis.com") {
    stderr.write(
      "[lineage] Skipped lineage emission: skip_reason=unsupported_universe " +
        "(Cloud Data Lineage API is not available when credentials.universeDomain " +
        "targets a non-default universe)\n",
    );
    return undefined;
  }

  const options: IEmitterOptions = {
    lineageEnabled: true,
    dryRun: input.dryRun,
    projectDir: input.projectDir,
    location: input.location,
  };
  return new LineageEmitter(input.readCredentials, options, clientProvider, stderr);
}
