import { createHash, randomUUID } from "crypto";
import Long from "long";
import * as path from "path";

import { dataform } from "df/protos/ts";

const PRODUCER_URL = "https://github.com/dataform-co/dataform";

/**
 * Assembles OpenLineage RunEvent payloads for Dataform actions.
 *
 * Stateful across the lifetime of one dataform run:
 * - `parentRunId` — one UUID per run, shared by every action's ParentRunFacet
 * - `activeRunIds` — maps action key → runId so START and terminal events
 *   share the same runId (OpenLineage requires this to correlate a run)
 * - `workdirHash` — lazily computed once from `projectDir`
 *
 * All state is derived from inputs. No I/O.
 */
export class LineagePayloadBuilder {
  private readonly projectDir?: string;
  private readonly parentRunId: string;
  private readonly activeRunIds = new Map<string, string>();
  private workdirHash: string = "";

  constructor(projectDir?: string) {
    this.projectDir = projectDir;
    this.parentRunId = randomUUID();
  }

  public build(
    action: dataform.IExecutionAction,
    actionResult: dataform.IActionResult,
    projectId: string,
    location: string
  ): { [key: string]: any } {
    if (!this.workdirHash && this.projectDir) {
      this.workdirHash = createHash("sha256").update(this.projectDir).digest("hex").slice(0, 16);
    }

    const eventTime = new Date().toISOString();
    const actionKey = `${action.target.database || ""}.${action.target.schema}.${action.target.name}`;
    let runId = this.activeRunIds.get(actionKey);
    if (!runId) {
      runId = randomUUID();
      this.activeRunIds.set(actionKey, runId);
    }

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
      name: `${dep.database || projectId}.${dep.schema}.${dep.name}`
    }));

    const outputs = [
      {
        namespace: "bigquery",
        name: `${action.target.database || projectId}.${action.target.schema}.${action.target.name}`
      }
    ];

    const workdirIdentifier = this.buildWorkdirIdentifier();
    const canonicalActionTarget = `${action.target.schema}.${action.target.name}`;
    const jobName = `${projectId}.${location}.cli.${workdirIdentifier}.${canonicalActionTarget}`;
    const parentJobName = `${projectId}.${location}.cli.${workdirIdentifier}.run`;

    const nominalTime: any = {
      _schemaURL: "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json",
      nominalStartTime: new Date(
        toMillis(actionResult.timing?.startTimeMillis) ?? Date.now()
      ).toISOString()
    };
    if (actionResult.timing?.endTimeMillis) {
      nominalTime.nominalEndTime = new Date(
        toMillis(actionResult.timing.endTimeMillis)
      ).toISOString();
    }

    const runFacets: any = {
      nominalTime,
      parent: {
        _producer: PRODUCER_URL,
        _schemaURL:
          "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet",
        job: {
          namespace: "dataform",
          name: parentJobName
        },
        run: {
          runId: this.parentRunId
        }
      },
      gcp_bq_pipelines_run: {
        runType: "cli-manual"
      }
    };

    const jobFacets: any = {};
    jobFacets.gcp_lineage = {
      _producer: PRODUCER_URL,
      _schemaURL:
        "https://openlineage.io/spec/facets/1-0-0/GcpLineageJobFacet.json#/$defs/GcpLineageJobFacet",
      displayName: `BigQuery Pipelines action ${canonicalActionTarget}`,
      origin: {
        name: `projects/${projectId}/locations/${location}/cli/${workdirIdentifier}`,
        sourceType: "BIGQUERY_PIPELINES"
      }
    };

    return {
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
      producer: PRODUCER_URL,
      schemaURL: "https://openlineage.io/spec/1-0-2/OpenLineage.json#/definitions/RunEvent"
    };
  }

  private buildWorkdirIdentifier(): string {
    if (!this.workdirHash || !this.projectDir) {
      return "unknown-workdir";
    }
    const rawBase = path.basename(this.projectDir);
    const sanitized = rawBase
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
    const base = sanitized || "workdir";
    const shortHash = this.workdirHash.slice(0, 8);
    return `${base}-${shortHash}`;
  }
}

function toMillis(val: Long | number | undefined | null): number | undefined {
  if (typeof val === "number") {
    return val;
  }
  if (val && typeof (val as Long).toNumber === "function") {
    return (val as Long).toNumber();
  }
  return undefined;
}

export function toProtoStruct(obj: { [key: string]: any }): any {
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
