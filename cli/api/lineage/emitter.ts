import { LineageClient } from "@google-cloud/lineage";

import { LineagePayloadBuilder, toProtoStruct } from "df/cli/api/lineage/payload_builder";
import { coerceAsError } from "df/common/errors/errors";
import { dataform } from "df/protos/ts";

const GLOBAL_LINEAGE_ENDPOINT = "datalineage.googleapis.com";

export interface IEmitterOptions {
  lineageEnabled?: boolean;
  dryRun?: boolean;
  projectDir?: string;
}

/**
 * Minimal writable stream shape used for stderr output. Injectable so tests can
 * capture emitted skip-reason lines deterministically.
 */
export interface IStderrLike {
  write(msg: string): unknown;
}

export type LineageClientProvider = (projectId: string, endpoint: string) => LineageClient;

export function createLineageClientProvider(
  credentials: dataform.IBigQuery
): LineageClientProvider {
  const clients = new Map<string, LineageClient>();
  return (projectId: string, endpoint: string) => {
    const targetProjectId = projectId || credentials.projectId;
    const cacheKey = `${targetProjectId}::${endpoint}`;
    if (!clients.has(cacheKey)) {
      clients.set(
        cacheKey,
        new LineageClient({
          projectId: targetProjectId,
          apiEndpoint: endpoint,
          credentials: credentials.credentials && JSON.parse(credentials.credentials)
        })
      );
    }
    return clients.get(cacheKey);
  };
}

export class LineageEmitter {
  private readonly clientProvider: LineageClientProvider;
  private readonly credentials: dataform.IBigQuery;
  private readonly emitterOptions: IEmitterOptions;
  private readonly stderr: IStderrLike;
  private readonly payloadBuilder: LineagePayloadBuilder;
  private readonly pending = new Set<Promise<void>>();
  private emissionDisabledThisRun = false;
  private dryRunSkipLogged = false;

  constructor(
    credentials: dataform.IBigQuery,
    emitterOptions: IEmitterOptions,
    clientProvider?: LineageClientProvider,
    stderr: IStderrLike = process.stderr
  ) {
    this.credentials = credentials;
    this.emitterOptions = emitterOptions;
    this.stderr = stderr;
    this.clientProvider = clientProvider || createLineageClientProvider(credentials);
    this.payloadBuilder = new LineagePayloadBuilder(emitterOptions.projectDir);
  }

  public emitForAction(
    action: dataform.IExecutionAction,
    actionResult: dataform.IActionResult
  ): void {
    if (this.emissionDisabledThisRun) {
      return;
    }

    if (this.emitterOptions.dryRun) {
      if (!this.dryRunSkipLogged) {
        this.stderr.write(
          "[lineage] Skipped lineage emission (dry-run mode; once-per-run): skip_reason=dry_run\n"
        );
        this.dryRunSkipLogged = true;
      }
      return;
    }

    // Non-table/non-operation actions (e.g., assertions, declarations) are not
    // emitted. This is a scope decision, not a user-visible misconfiguration,
    // so it is intentionally silent.
    const isEligibleType = action.type === "table" || action.type === "operation";
    if (!isEligibleType) {
      return;
    }

    const p = this.emitForActionInternal(action, actionResult)
      .catch(e => {
        const err = coerceAsError(e);
        this.emissionDisabledThisRun = true;
        this.stderr.write(
          `[lineage] Failed to emit lineage for action ${action.target.schema}.${action.target.name}: ${err.message}\n`
        );
      })
      .finally(() => {
        this.pending.delete(p);
      });
    this.pending.add(p);
  }

  public async drain(maxWaitMs = 15000): Promise<void> {
    if (this.pending.size === 0) {
      return;
    }
    await Promise.race([
      Promise.allSettled([...this.pending]),
      new Promise<void>(resolve => setTimeout(resolve, maxWaitMs))
    ]);
  }

  private async emitForActionInternal(
    action: dataform.IExecutionAction,
    actionResult: dataform.IActionResult
  ): Promise<void> {
    const projectId = action.target.database || this.credentials.projectId;
    const location = (this.credentials.location || "US").toLowerCase();
    const parent = `projects/${projectId}/locations/${location}`;

    const openLineagePayload = this.payloadBuilder.build(
      action,
      actionResult,
      projectId,
      location,
      this.credentials.projectId
    );

    const client = this.clientProvider(projectId, GLOBAL_LINEAGE_ENDPOINT);
    await client.processOpenLineageRunEvent({
      parent,
      openLineage: toProtoStruct(openLineagePayload) as any
    });
  }
}
