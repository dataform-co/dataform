import { LineageClient } from "@google-cloud/lineage";
import { createHash } from "crypto";

import { coerceAsError } from "df/common/errors/errors";
import { dataform } from "df/protos/ts";

export interface IEmitterOptions {
  lineageEnabled?: boolean;
  dryRun?: boolean;
  projectDir?: string;
  apiEndpoint?: string;
}

export type LineageClientProvider = (projectId?: string) => LineageClient;

export function createLineageClientProvider(
  credentials: dataform.IBigQuery,
  apiEndpoint?: string
): LineageClientProvider {
  const clients = new Map<string, LineageClient>();
  return (projectId?: string) => {
    const targetProjectId = projectId || credentials.projectId;
    if (!clients.has(targetProjectId)) {
      clients.set(
        targetProjectId,
        new LineageClient({
          projectId: targetProjectId,
          apiEndpoint,
          credentials: credentials.credentials && JSON.parse(credentials.credentials)
        })
      );
    }
    return clients.get(targetProjectId);
  };
}

export class LineageEmitter {
  private readonly clientProvider: LineageClientProvider;
  private readonly credentials: dataform.IBigQuery;
  private readonly emitterOptions: IEmitterOptions;
  private readonly pending = new Set<Promise<void>>();
  private apiDisabledThisRun = false;
  private workdirHash: string = "";
  private readonly activeRunIds = new Map<string, string>();

  constructor(
    credentials: dataform.IBigQuery,
    emitterOptions: IEmitterOptions,
    clientProvider?: LineageClientProvider
  ) {
    this.credentials = credentials;
    this.emitterOptions = emitterOptions;
    this.clientProvider =
      clientProvider || createLineageClientProvider(credentials, emitterOptions.apiEndpoint);
  }

  public emitForAction(
    action: dataform.IExecutionAction,
    actionResult: dataform.IActionResult
  ): void {
    if (this.apiDisabledThisRun) {
      return;
    }

    // Eligibility check
    const isEnabled = this.emitterOptions.lineageEnabled ?? false;
    const isDryRun = this.emitterOptions.dryRun ?? false;
    const isEligibleType = action.type === "table" || action.type === "operation";

    if (!isEnabled || isDryRun || !isEligibleType) {
      return;
    }

    const p = this.emitForActionInternal(action, actionResult)
      .catch(e => {
        process.stderr.write(
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
    const jobName = `${projectId}/${location}/cli/${repositoryName}/${canonicalActionTarget}`;

    // Retrieve SQL facets if tasks exist
    const sqlStatements = action.tasks
      ?.map(task => task.statement)
      .filter(stmt => !!stmt)
      .join(";\n");

    const runFacets: any = {
      nominalTime: {
        _schemaURL: "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json",
        nominalStartTime: new Date(
          actionResult.timing?.startTimeMillis?.toNumber?.() || Date.now()
        ).toISOString()
      }
    };

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

    process.stderr.write(
      `[lineage-debug] Sending OpenLineage event:\n${JSON.stringify(openLineagePayload, null, 2)}\n`
    );

    // 2. Emit payload via ProcessOpenLineageRunEvent RPC with retry logic
    let attempts = 0;
    const maxAttempts = 2;
    while (attempts < maxAttempts) {
      attempts++;
      try {
        const client = this.clientProvider(projectId);
        await client.processOpenLineageRunEvent({
          parent,
          openLineage: toProtoStruct(openLineagePayload) as any
        });
        break;
      } catch (e) {
        const err = coerceAsError(e);
        const code = (err as any).code;

        // Check for permission or API disabled status codes
        if (code === 7 || err.message?.includes("PERMISSION_DENIED")) {
          process.stderr.write(
            `[lineage] Permission check failed. Ensure the credential has 'datalineage.googleapis.com/locations.processOpenLineageMessage'. Disabling lineage for this run.\n`
          );
          this.apiDisabledThisRun = true;
          return;
        } else if (
          code === 9 ||
          err.message?.includes("SERVICE_DISABLED") ||
          err.message?.includes("FAILED_PRECONDITION")
        ) {
          process.stderr.write(
            `[lineage] Lineage API is not enabled in project ${projectId}. Run 'gcloud services enable datalineage.googleapis.com'. Disabling lineage for this run.\n`
          );
          this.apiDisabledThisRun = true;
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
