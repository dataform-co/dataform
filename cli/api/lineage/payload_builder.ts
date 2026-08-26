import { createHash, randomUUID } from "crypto";
import Long from "long";
import * as path from "path";

import { version } from "df/core/version";
import { dataform } from "df/protos/ts";

const PRODUCER_URL = "https://github.com/dataform-co/dataform";

const NOMINAL_TIME_FACET_SCHEMA =
  "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json";
const PARENT_RUN_FACET_SCHEMA =
  "https://openlineage.io/spec/facets/1-2-0/ParentRunFacet.json#/$defs/ParentRunFacet";
const EXTERNAL_QUERY_FACET_SCHEMA =
  "https://openlineage.io/spec/facets/1-0-2/ExternalQueryRunFacet.json";
const ERROR_MESSAGE_FACET_SCHEMA =
  "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json";
const SQL_JOB_FACET_SCHEMA = "https://openlineage.io/spec/facets/1-1-0/SQLJobFacet.json";
const GCP_LINEAGE_JOB_FACET_SCHEMA =
  "https://openlineage.io/spec/facets/1-0-0/GcpLineageJobFacet.json#/$defs/GcpLineageJobFacet";
const JOB_TYPE_FACET_SCHEMA =
  "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json#/$defs/JobTypeJobFacet";
const RUN_EVENT_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

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
    location: string,
    credentialsProjectId?: string
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
      _schemaURL: NOMINAL_TIME_FACET_SCHEMA,
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
        _schemaURL: PARENT_RUN_FACET_SCHEMA,
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

    if (eventType !== "START") {
      const bqJobId = extractBqJobId(actionResult);
      if (bqJobId && credentialsProjectId) {
        runFacets.externalQuery = {
          _producer: PRODUCER_URL,
          _schemaURL: EXTERNAL_QUERY_FACET_SCHEMA,
          externalQueryId: `${credentialsProjectId}.${location}.${bqJobId}`,
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
          _schemaURL: ERROR_MESSAGE_FACET_SCHEMA,
          message: errorMessages,
          programmingLanguage: "typescript"
        };
      }
    }

    const sqlStatements = action.tasks
      ?.map(task => task.statement)
      .filter(stmt => !!stmt)
      .join(";\n");

    const jobFacets: any = {};
    if (sqlStatements) {
      jobFacets.sql = {
        _schemaURL: SQL_JOB_FACET_SCHEMA,
        query: sqlStatements
      };
    }

    jobFacets.gcp_lineage = {
      _producer: PRODUCER_URL,
      _schemaURL: GCP_LINEAGE_JOB_FACET_SCHEMA,
      displayName: `BigQuery Pipelines action ${canonicalActionTarget}`,
      origin: {
        name: `projects/${projectId}/locations/${location}/cli/${workdirIdentifier}`,
        sourceType: "BIGQUERY_PIPELINES"
      }
    };

    jobFacets.jobType = {
      _producer: PRODUCER_URL,
      _schemaURL: JOB_TYPE_FACET_SCHEMA,
      integration: "BIGQUERY_PIPELINES",
      jobType: "ACTION",
      processingType: "BATCH"
    };

    jobFacets.gcp_bq_pipelines_job = {
      dataformCoreVersion: version,
      actionType: action.type,
      actionName: canonicalActionTarget
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
      schemaURL: RUN_EVENT_SCHEMA
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

function extractBqJobId(actionResult: dataform.IActionResult): string | undefined {
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
