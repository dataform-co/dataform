import { expect } from "chai";
import Long from "long";

import { LineagePayloadBuilder } from "df/cli/api/lineage/payload_builder";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

const ACTION = dataform.ExecutionAction.create({
  target: { database: "proj", schema: "schema", name: "table" },
  type: "table",
  tasks: [{ statement: "SELECT 1" }],
});

const START_RESULT = dataform.ActionResult.create({
  status: dataform.ActionResult.ExecutionStatus.RUNNING,
  timing: { startTimeMillis: Long.fromNumber(1_700_000_000_000) },
});

const COMPLETE_RESULT = dataform.ActionResult.create({
  status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
  timing: {
    startTimeMillis: Long.fromNumber(1_700_000_000_000),
    endTimeMillis: Long.fromNumber(1_700_000_010_000),
  },
  tasks: [{ metadata: { bigquery: { jobId: "job-abc" } } }],
});

suite("LineagePayloadBuilder", () => {
  test("maps ActionResult status to OpenLineage eventType", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const running = builder.build(ACTION, START_RESULT, "proj", "us");
    const successful = builder.build(ACTION, COMPLETE_RESULT, "proj", "us");
    const failed = builder.build(
      ACTION,
      dataform.ActionResult.create({ status: dataform.ActionResult.ExecutionStatus.FAILED }),
      "proj",
      "us",
    );
    const cancelled = builder.build(
      ACTION,
      dataform.ActionResult.create({ status: dataform.ActionResult.ExecutionStatus.CANCELLED }),
      "proj",
      "us",
    );
    expect(running.eventType).to.equal("START");
    expect(successful.eventType).to.equal("COMPLETE");
    expect(failed.eventType).to.equal("FAIL");
    expect(cancelled.eventType).to.equal("ABORT");
  });

  test("correlates START and terminal event via shared runId per action", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const start = builder.build(ACTION, START_RESULT, "proj", "us");
    const complete = builder.build(ACTION, COMPLETE_RESULT, "proj", "us");
    expect(start.run.runId).to.equal(complete.run.runId);
  });

  test("issues a fresh runId for the next START on the same action after terminal", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const firstStart = builder.build(ACTION, START_RESULT, "proj", "us");
    builder.build(ACTION, COMPLETE_RESULT, "proj", "us");
    const secondStart = builder.build(ACTION, START_RESULT, "proj", "us");
    expect(secondStart.run.runId).to.not.equal(firstStart.run.runId);
  });

  test("shares parentRunId across different actions in the same builder", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const otherAction = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "other" },
      type: "table",
    });
    const first = builder.build(ACTION, START_RESULT, "proj", "us");
    const second = builder.build(otherAction, START_RESULT, "proj", "us");
    expect(first.run.facets.parent.run.runId).to.equal(second.run.facets.parent.run.runId);
  });

  test("workdirIdentifier includes sanitized basename + 8-char hash prefix", () => {
    const builder = new LineagePayloadBuilder("/tmp/My_Project.Dir");
    const payload = builder.build(ACTION, START_RESULT, "proj", "us");
    // gcp_lineage.origin.name is the only place workdirIdentifier is spliced in.
    const originName = payload.job.facets.gcp_lineage.origin.name;
    expect(originName).to.match(/^projects\/proj\/locations\/us\/cli\/my-project-dir-[0-9a-f]{8}$/);
  });

  test("workdirIdentifier falls back to 'unknown-workdir' with no projectDir", () => {
    const builder = new LineagePayloadBuilder();
    const payload = builder.build(ACTION, START_RESULT, "proj", "us");
    const originName = payload.job.facets.gcp_lineage.origin.name;
    expect(originName).to.equal("projects/proj/locations/us/cli/unknown-workdir");
  });

  test("externalQuery run facet appears only on terminal events with a bqJobId", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const start = builder.build(ACTION, START_RESULT, "proj", "us", "cred-proj");
    const complete = builder.build(ACTION, COMPLETE_RESULT, "proj", "us", "cred-proj");
    expect(start.run.facets.externalQuery).to.equal(undefined);
    expect(complete.run.facets.externalQuery).to.deep.include({
      externalQueryId: "cred-proj.us.job-abc",
      source: "bigquery",
    });
  });

  test("externalQuery omitted when credentialsProjectId not passed even with bqJobId", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const complete = builder.build(ACTION, COMPLETE_RESULT, "proj", "us");
    expect(complete.run.facets.externalQuery).to.equal(undefined);
  });

  test("externalQuery uses the last non-assertion task job ID", () => {
    const multiTaskTable = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table",
      tasks: [
        { type: "statement", statement: "CREATE SCHEMA IF NOT EXISTS ..." },
        { type: "statement", statement: "CREATE OR REPLACE TABLE ..." },
      ],
    });
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const complete = builder.build(
      multiTaskTable,
      dataform.ActionResult.create({
        status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        tasks: [
          { metadata: { bigquery: { jobId: "job-schema" } } },
          { metadata: { bigquery: { jobId: "job-ctas" } } },
        ],
      }),
      "proj",
      "us",
      "cred-proj",
    );
    expect(complete.run.facets.externalQuery.externalQueryId).to.equal("cred-proj.us.job-ctas");
  });

  test("externalQuery uses the CREATE VIEW job for assertions, not the check job", () => {
    const assertion = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "assertions", name: "assert_a" },
      type: "assertion",
      tasks: [
        {
          type: "statement",
          statement: "create or replace view `proj.assertions.assert_a` as ...",
        },
        {
          type: "assertion",
          statement: "select sum(1) as row_count from `proj.assertions.assert_a`",
        },
      ],
    });
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const tasks = [
      { metadata: { bigquery: { jobId: "job-create-view" } } },
      { metadata: { bigquery: { jobId: "job-check" } } },
    ];
    const complete = builder.build(
      assertion,
      dataform.ActionResult.create({
        status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
        tasks,
      }),
      "proj",
      "us",
      "cred-proj",
    );
    const failed = builder.build(
      assertion,
      dataform.ActionResult.create({
        status: dataform.ActionResult.ExecutionStatus.FAILED,
        tasks,
      }),
      "proj",
      "us",
      "cred-proj",
    );
    expect(complete.run.facets.externalQuery.externalQueryId).to.equal(
      "cred-proj.us.job-create-view",
    );
    expect(failed.run.facets.externalQuery.externalQueryId).to.equal(
      "cred-proj.us.job-create-view",
    );
  });

  test("does not emit sql job facet or errorMessage run facet", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const failed = builder.build(
      ACTION,
      dataform.ActionResult.create({
        status: dataform.ActionResult.ExecutionStatus.FAILED,
        tasks: [{ errorMessage: "boom" }],
      }),
      "proj",
      "us",
    );
    expect(failed.job.facets.sql).to.equal(undefined);
    expect(failed.run.facets.errorMessage).to.equal(undefined);
  });

  test("jobType job facet declares BigQuery Pipelines integration", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const payload = builder.build(ACTION, START_RESULT, "proj", "us");
    expect(payload.job.facets.jobType).to.deep.include({
      integration: "BIGQUERY_PIPELINES",
      jobType: "ACTION",
      processingType: "BATCH",
    });
  });

  test("gcp_bq_pipelines_job facet carries dataformCoreVersion + action metadata", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const payload = builder.build(ACTION, START_RESULT, "fallback-proj", "us");
    expect(payload.job.facets.gcp_bq_pipelines_job).to.deep.include({
      actionType: "TABLE",
      actionName: "proj.schema.table",
    });
    expect(payload.job.facets.gcp_lineage.displayName).to.equal(
      "BigQuery Pipelines action proj.schema.table",
    );
    expect(payload.job.facets.gcp_bq_pipelines_job.dataformCoreVersion).to.be.a("string");
  });

  test("actionName and displayName fall back to projectId when database is omitted", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const actionWithoutDatabase = dataform.ExecutionAction.create({
      target: { schema: "schema", name: "table" },
      type: "table",
    });
    const fallbackPayload = builder.build(
      actionWithoutDatabase,
      START_RESULT,
      "fallback-proj",
      "us",
    );
    expect(fallbackPayload.job.facets.gcp_bq_pipelines_job.actionName).to.equal(
      "fallback-proj.schema.table",
    );
    expect(fallbackPayload.job.facets.gcp_lineage.displayName).to.equal(
      "BigQuery Pipelines action fallback-proj.schema.table",
    );
  });

  test("throws when called with an unsupported action type", () => {
    const declaration = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "decl" },
      type: "declaration",
    });
    const builder = new LineagePayloadBuilder("/tmp/proj");
    expect(() => builder.build(declaration, START_RESULT, "proj", "us")).to.throw(
      /Unsupported action type for lineage: declaration/,
    );
  });

  test("inputs and outputs are populated on COMPLETE and empty on START, FAIL, and ABORT", () => {
    const actionWithDeps = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table",
      dependencyTargets: [
        { database: "p1", schema: "s1", name: "n1" },
        { schema: "s2", name: "n2" },
      ],
    });
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const start = builder.build(actionWithDeps, START_RESULT, "fallback-proj", "us");
    const complete = builder.build(actionWithDeps, COMPLETE_RESULT, "fallback-proj", "us");
    const fail = builder.build(
      actionWithDeps,
      dataform.ActionResult.create({ status: dataform.ActionResult.ExecutionStatus.FAILED }),
      "fallback-proj",
      "us",
    );
    const abort = builder.build(
      actionWithDeps,
      dataform.ActionResult.create({ status: dataform.ActionResult.ExecutionStatus.CANCELLED }),
      "fallback-proj",
      "us",
    );

    expect(start.inputs).to.deep.equal([]);
    expect(start.outputs).to.deep.equal([]);
    expect(fail.inputs).to.deep.equal([]);
    expect(fail.outputs).to.deep.equal([]);
    expect(abort.inputs).to.deep.equal([]);
    expect(abort.outputs).to.deep.equal([]);

    expect(complete.inputs).to.deep.equal([
      { namespace: "bigquery", name: "p1.s1.n1" },
      { namespace: "bigquery", name: "fallback-proj.s2.n2" },
    ]);
    expect(complete.outputs).to.deep.equal([{ namespace: "bigquery", name: "proj.schema.table" }]);
  });

  test("inputs and outputs are emitted on COMPLETE for actions with output and omitted for operations without hasOutput", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const deps = [{ database: "p1", schema: "s1", name: "n1" }];

    for (const typeWithOutput of ["table", "assertion", "propertyGraph"]) {
      const action = dataform.ExecutionAction.create({
        target: { database: "proj", schema: "schema", name: "target_name" },
        type: typeWithOutput,
        dependencyTargets: deps,
      });
      const complete = builder.build(action, COMPLETE_RESULT, "proj", "us");
      expect(complete.inputs, typeWithOutput).to.deep.equal([
        { namespace: "bigquery", name: "p1.s1.n1" },
      ]);
      expect(complete.outputs, typeWithOutput).to.deep.equal([
        { namespace: "bigquery", name: "proj.schema.target_name" },
      ]);
    }

    const opWithOutput = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "op_out" },
      type: "operation",
      hasOutput: true,
      dependencyTargets: deps,
    });
    const opWithOutputComplete = builder.build(opWithOutput, COMPLETE_RESULT, "proj", "us");
    expect(opWithOutputComplete.inputs).to.deep.equal([
      { namespace: "bigquery", name: "p1.s1.n1" },
    ]);
    expect(opWithOutputComplete.outputs).to.deep.equal([
      { namespace: "bigquery", name: "proj.schema.op_out" },
    ]);

    const opWithoutOutput = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "op_no_out" },
      type: "operation",
      hasOutput: false,
      dependencyTargets: deps,
    });
    const opWithoutOutputComplete = builder.build(opWithoutOutput, COMPLETE_RESULT, "proj", "us");
    expect(opWithoutOutputComplete.inputs).to.deep.equal([]);
    expect(opWithoutOutputComplete.outputs).to.deep.equal([]);
  });
});
