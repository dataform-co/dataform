import { LineageClient } from "@google-cloud/lineage";
import { createHash } from "crypto";

import { coerceAsError } from "df/common/errors/errors";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

export interface IEmitterOptions {
  lineageEnabled?: boolean;
  dryRun?: boolean;
  projectDir?: string;
  apiEndpoint?: string;
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
  credentials: dataform.IBigQuery,
  apiEndpointOverride?: string
): LineageClientProvider {
  const clients = new Map<string, LineageClient>();
  return (projectId: string, endpoint: string) => {
    const targetProjectId = projectId || credentials.projectId;
    const effectiveEndpoint = apiEndpointOverride || endpoint;
    const cacheKey = `${targetProjectId}::${effectiveEndpoint}`;
    if (!clients.has(cacheKey)) {
      clients.set(
        cacheKey,
        new LineageClient({
          projectId: targetProjectId,
          apiEndpoint: effectiveEndpoint,
          credentials: credentials.credentials && JSON.parse(credentials.credentials)
        })
      );
    }
    return clients.get(cacheKey);
  };
}

const GLOBAL_LINEAGE_ENDPOINT = "datalineage.googleapis.com";

function regionalEndpointFor(location: string): string {
  return `datalineage.${location}.rep.googleapis.com`;
}

export class LineageEmitter {
  private readonly clientProvider: LineageClientProvider;
  private readonly credentials: dataform.IBigQuery;
  private readonly emitterOptions: IEmitterOptions;
  private readonly stderr: IStderrLike;
  private readonly pending = new Set<Promise<void>>();
  private apiDisabledThisRun = false;
  private dryRunSkipLogged = false;
  private workdirHash: string = "";
  private readonly activeRunIds = new Map<string, string>();
  private readonly parentRunId: string;
  private readonly repUnavailableForLocation = new Set<string>();

  constructor(
    credentials: dataform.IBigQuery,
    emitterOptions: IEmitterOptions,
    clientProvider?: LineageClientProvider,
    stderr: IStderrLike = process.stderr
  ) {
    this.credentials = credentials;
    this.emitterOptions = emitterOptions;
    this.stderr = stderr;
    this.clientProvider =
      clientProvider || createLineageClientProvider(credentials, emitterOptions.apiEndpoint);
    this.parentRunId = this.generateUuid();
  }

  public emitForAction(
    action: dataform.IExecutionAction,
    actionResult: dataform.IActionResult
  ): void {
    if (this.apiDisabledThisRun) {
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
        this.stderr.write(
          `[lineage] Failed to emit lineage for action ${action.target.schema}.${action.target.name}: ${e.message}\n`
        );
      })
      .finally(() => {
        this.pending.delete(p);
      });
    this.pending.add(p);
  }

  public async drain(maxWaitMs = 10000): Promise<void> {
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

    // Initialize workdir hash if not done
    if (!this.workdirHash && this.emitterOptions.projectDir) {
      this.workdirHash = createHash("sha256")
        .update(this.emitterOptions.projectDir)
        .digest("hex")
        .slice(0, 16);
    }

    // 1. Build OpenLineage payload
    const eventTime = new Date().toISOString();
    const actionKey = `${action.target.database || ""}.${action.target.schema}.${action.target.name}`;
    let runId = this.activeRunIds.get(actionKey);
    if (!runId) {
      runId = this.generateUuid();
      this.activeRunIds.set(actionKey, runId);
    }

    // Map Action Status to OpenLineage eventType
    let eventType: "START" | "COMPLETE" | "FAIL" | "ABORT" = "START";
    if (actionResult.status === dataform.ActionResult.ExecutionStatus.RUNNING) {
      eventType = "START";
    } else {
      this.activeRunIds.delete(actionKey);
      if (actionResult.status === dataform.ActionResult.ExecutionStatus.FAILED) {
        eventType = "FAIL";
      } else if (actionResult.status === dataform.ActionResult.ExecutionStatus.CANCELLED) {
        eventType = "ABORT";
      } else if (actionResult.status === dataform.ActionResult.ExecutionStatus.SUCCESSFUL) {
        eventType = "COMPLETE";
      }
    }

    const inputs = (action.dependencyTargets || []).map(dep => ({
      namespace: "bigquery",
      name: `${dep.database}.${dep.schema}.${dep.name}`
    }));

    const outputs = [
      {
        namespace: "bigquery",
        name: `${action.target.database}.${action.target.schema}.${action.target.name}`
      }
    ];

    // Job and Run names
    const repositoryName = this.workdirHash || "unknown-repo";
    const canonicalActionTarget = `${action.target.schema}.${action.target.name}`;
    const jobName = `${projectId}.${location}.cli.${repositoryName}.${canonicalActionTarget}`;
    const parentJobName = `${projectId}.${location}.cli.${repositoryName}.run`;

    const nominalTime: any = {
      _schemaURL: "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json",
      nominalStartTime: new Date(
        actionResult.timing?.startTimeMillis?.toNumber?.() || Date.now()
      ).toISOString()
    };
    if (actionResult.timing?.endTimeMillis) {
      nominalTime.nominalEndTime = new Date(
        actionResult.timing.endTimeMillis.toNumber()
      ).toISOString();
    }

    const runFacets: any = {
      nominalTime,
      parent: {
        _producer: "https://github.com/dataform-co/dataform",
        _schemaURL: "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet",
        job: {
          namespace: "dataform",
          name: parentJobName
        },
        run: {
          runId: this.parentRunId
        }
      },
      gcp_bq_pipelines_run: {
        runType: "cli"
      }
    };

    // Enables the Dataplex Lineage UI's "BigQuery Job ID" field for CLI runs.
    if (eventType !== "START") {
      const bqJobId = LineageEmitter.extractBqJobId(actionResult);
      const jobProjectId = this.credentials.projectId;
      if (bqJobId && jobProjectId) {
        runFacets.externalQuery = {
          _producer: "https://github.com/dataform-co/dataform",
          _schemaURL: "https://openlineage.io/spec/facets/1-0-0/ExternalQueryRunFacet.json",
          externalQueryId: `${jobProjectId}.${location}.${bqJobId}`,
          source: "bigquery"
        };
      }
    }

    if (eventType === "FAIL") {
      const errorMessages = actionResult.tasks
        ?.map(t => t.errorMessage)
        .filter(msg => !!msg)
        .join("; ");
      if (errorMessages) {
        runFacets.errorMessage = {
          _schemaURL: "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
          message: errorMessages,
          programmingLanguage: "typescript"
        };
      }
    }

    // Retrieve SQL facets if tasks exist
    const sqlStatements = action.tasks
      ?.map(task => task.statement)
      .filter(stmt => !!stmt)
      .join(";\n");

    const jobFacets: any = {};
    if (sqlStatements) {
      jobFacets.sql = {
        _schemaURL: "https://openlineage.io/spec/facets/1-0-0/SqlJobFacet.json",
        query: sqlStatements
      };
    }

    if (action.fileName) {
      jobFacets.sourceCodeLocation = {
        _schemaURL: "https://openlineage.io/spec/facets/1-0-0/SourceCodeLocationJobFacet.json",
        type: "git",
        url: action.fileName
      };
    }

    jobFacets.gcp_lineage = {
      _producer: "https://github.com/dataform-co/dataform",
      _schemaURL: "https://openlineage.io/spec/facets/1-0-0/GcpLineageJobFacet.json#/$defs/GcpLineageJobFacet",
      displayName: `BQ Pipelines action ${canonicalActionTarget}`,
      origin: {
        name: `projects/${projectId}/locations/${location}/cli/${repositoryName}`,
        sourceType: "BQ_PIPELINES"
      }
    };

    jobFacets.jobType = {
      _producer: "https://github.com/dataform-co/dataform",
      _schemaURL: "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
      integration: "BQ_PIPELINES",
      jobType: "ACTION",
      processingType: "BATCH"
    };

    jobFacets.gcp_bq_pipelines_job = {
      dataformCoreVersion: version,
      actionType: action.type,
      actionName: canonicalActionTarget
    };

    const openLineagePayload = {
      eventType,
      eventTime,
      run: {
        runId,
        facets: runFacets
      },
      job: {
        namespace: "dataform",
        name: jobName,
        facets: jobFacets
      },
      inputs,
      outputs,
      producer: "https://github.com/dataform-co/dataform",
      schemaURL: "https://openlineage.io/spec/1-0-2/OpenLineage.json#/definitions/RunEvent"
    };

    this.stderr.write(
      `[lineage-debug] Sending OpenLineage event:\n${JSON.stringify(openLineagePayload, null, 2)}\n`
    );

    // 2. Emit payload via ProcessOpenLineageRunEvent RPC with retry logic.
    // Uses REP endpoint (data-residency-preserving) by default and falls back
    // to the global endpoint if REP for this location is not resolvable — some
    // locations (e.g. certain multi-regions, Omni) are not turned up in REP
    // infrastructure. The fallback decision is cached per-location for the
    // lifetime of the emitter so subsequent emits skip the failing lookup.
    let currentEndpoint = this.endpointForLocation(location);
    let attempts = 0;
    const maxAttempts = 2;
    while (attempts < maxAttempts) {
      attempts++;
      try {
        const client = this.clientProvider(projectId, currentEndpoint);
        await client.processOpenLineageRunEvent(
          {
            parent,
            openLineage: toProtoStruct(openLineagePayload) as any
          },
          { timeout: 2000 }
        );
        break;
      } catch (e) {
        const err = coerceAsError(e);
        const code = (err as any).code;

        // Fall back from REP to global if the REP hostname isn't resolvable.
        const overrideActive = !!this.emitterOptions.apiEndpoint;
        const onRepEndpoint = currentEndpoint !== GLOBAL_LINEAGE_ENDPOINT;
        if (
          !overrideActive &&
          onRepEndpoint &&
          this.isEndpointUnresolvable(err)
        ) {
          this.stderr.write(
            `[lineage] Regional endpoint ${currentEndpoint} is not resolvable for location ${location}. Falling back to ${GLOBAL_LINEAGE_ENDPOINT} for this and subsequent emits in this location.\n`
          );
          this.repUnavailableForLocation.add(location);
          currentEndpoint = GLOBAL_LINEAGE_ENDPOINT;
          attempts--;
          continue;
        }

        // Check for permission or API disabled status codes. Multiple in-flight
        // calls can hit the same failure concurrently; guard the write so the
        // skip line is printed at most once per run.
        if (code === 7 || err.message?.includes("PERMISSION_DENIED")) {
          if (!this.apiDisabledThisRun) {
            this.apiDisabledThisRun = true;
            this.stderr.write(
              "[lineage] Skipped lineage emission for the rest of this run: skip_reason=api_disabled (permission check failed; ensure the credential has 'datalineage.googleapis.com/locations.processOpenLineageMessage')\n"
            );
          }
          return;
        } else if (
          code === 9 ||
          err.message?.includes("SERVICE_DISABLED") ||
          err.message?.includes("FAILED_PRECONDITION")
        ) {
          if (!this.apiDisabledThisRun) {
            this.apiDisabledThisRun = true;
            this.stderr.write(
              `[lineage] Skipped lineage emission for the rest of this run: skip_reason=api_disabled (Lineage API is not enabled in project ${projectId}; run 'gcloud services enable datalineage.googleapis.com')\n`
            );
          }
          return;
        }

        const isTransient =
          code === 14 ||
          code === 4 ||
          err.message?.includes("UNAVAILABLE") ||
          err.message?.includes("DEADLINE_EXCEEDED");

        if (isTransient && attempts < maxAttempts) {
          await new Promise(resolve => setTimeout(resolve, 500));
          continue;
        }
        throw err;
      }
    }
  }

  private endpointForLocation(location: string): string {
    if (this.emitterOptions.apiEndpoint) {
      return this.emitterOptions.apiEndpoint;
    }
    if (this.repUnavailableForLocation.has(location)) {
      return GLOBAL_LINEAGE_ENDPOINT;
    }
    return regionalEndpointFor(location);
  }

  private isEndpointUnresolvable(err: Error): boolean {
    // Match on the DNS signature in the message string rather than the outer
    // grpc code. google-gax wraps repeated UNAVAILABLE(14) errors from the
    // grpc DNS resolver in an outer DEADLINE_EXCEEDED(4) once its retry
    // budget expires; the inner "Name resolution failed" text is only in the
    // message. The signatures below are unique enough on their own that a
    // false positive is not a concern.
    const cause = (err as any).cause;
    const causeMessage = typeof cause === "object" && cause ? String(cause.message || cause.code || "") : "";
    const combined = `${err.message || ""} ${causeMessage}`;
    return /ENOTFOUND|EAI_AGAIN|getaddrinfo|(?:DNS|Name) resolution failed/i.test(combined);
  }

  private static extractBqJobId(actionResult: dataform.IActionResult): string | undefined {
    if (!actionResult.tasks) {
      return undefined;
    }
    for (let i = actionResult.tasks.length - 1; i >= 0; i--) {
      const jobId = actionResult.tasks[i]?.metadata?.bigquery?.jobId;
      if (jobId) {
        return jobId;
      }
    }
    return undefined;
  }

  private generateUuid(): string {
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, c => {
      const r = Math.floor(Math.random() * 16);
      const v = c === "x" ? r : (r % 4) + 8;
      return v.toString(16);
    });
  }
}

function toProtoStruct(obj: { [key: string]: any }): any {
  const fields: { [key: string]: any } = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    if (val !== undefined) {
      fields[key] = toProtoValue(val);
    }
  }
  return { fields };
}

function toProtoValue(val: any): any {
  if (val === null) {
    return { nullValue: 0 };
  }
  if (typeof val === "string") {
    return { stringValue: val };
  }
  if (typeof val === "number") {
    return { numberValue: val };
  }
  if (typeof val === "boolean") {
    return { boolValue: val };
  }
  if (Array.isArray(val)) {
    return { listValue: { values: val.map(toProtoValue) } };
  }
  if (typeof val === "object") {
    return { structValue: toProtoStruct(val) };
  }
  return { nullValue: 0 };
}
