import { expect } from "chai";
import Long from "long";

import { LineagePayloadBuilder } from "df/cli/api/lineage/payload_builder";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

const ACTION = dataform.ExecutionAction.create({
  target: { database: "proj", schema: "schema", name: "table" },
  type: "table",
  tasks: [{ statement: "SELECT 1" }]
});

const START_RESULT = dataform.ActionResult.create({
  status: dataform.ActionResult.ExecutionStatus.RUNNING,
  timing: { startTimeMillis: Long.fromNumber(1_700_000_000_000) }
});

const COMPLETE_RESULT = dataform.ActionResult.create({
  status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
  timing: {
    startTimeMillis: Long.fromNumber(1_700_000_000_000),
    endTimeMillis: Long.fromNumber(1_700_000_010_000)
  },
  tasks: [{ metadata: { bigquery: { jobId: "job-abc" } } }]
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
      "us"
    );
    const cancelled = builder.build(
      ACTION,
      dataform.ActionResult.create({ status: dataform.ActionResult.ExecutionStatus.CANCELLED }),
      "proj",
      "us"
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
      type: "table"
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

  test("externalQuery facet appears only on terminal events with a bqJobId", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const start = builder.build(ACTION, START_RESULT, "proj", "us", "cred-proj");
    const complete = builder.build(ACTION, COMPLETE_RESULT, "proj", "us", "cred-proj");
    expect(start.run.facets.externalQuery).to.equal(undefined);
    expect(complete.run.facets.externalQuery).to.deep.include({
      externalQueryId: "cred-proj.us.job-abc",
      source: "bigquery"
    });
  });

  test("errorMessage facet appears only on FAIL with non-empty task errorMessage", () => {
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const failedWithMsg = builder.build(
      ACTION,
      dataform.ActionResult.create({
        status: dataform.ActionResult.ExecutionStatus.FAILED,
        tasks: [{ errorMessage: "boom" }, { errorMessage: "kaboom" }]
      }),
      "proj",
      "us"
    );
    expect(failedWithMsg.run.facets.errorMessage).to.deep.include({
      message: "boom; kaboom",
      programmingLanguage: "typescript"
    });
    const failedWithoutMsg = builder.build(
      ACTION,
      dataform.ActionResult.create({
        status: dataform.ActionResult.ExecutionStatus.FAILED
      }),
      "proj",
      "us"
    );
    expect(failedWithoutMsg.run.facets.errorMessage).to.equal(undefined);
  });

  test("inputs list mirrors action.dependencyTargets in bigquery namespace", () => {
    const actionWithDeps = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table",
      dependencyTargets: [
        { database: "p1", schema: "s1", name: "n1" },
        { database: "p2", schema: "s2", name: "n2" }
      ]
    });
    const builder = new LineagePayloadBuilder("/tmp/proj");
    const payload = builder.build(actionWithDeps, START_RESULT, "proj", "us");
    expect(payload.inputs).to.deep.equal([
      { namespace: "bigquery", name: "p1.s1.n1" },
      { namespace: "bigquery", name: "p2.s2.n2" }
    ]);
  });
});
