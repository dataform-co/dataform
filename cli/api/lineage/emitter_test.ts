import { expect } from "chai";
import Long from "long";

import { LineageEmitter } from "df/cli/api/lineage/emitter";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

class StderrCapture {
  public writes: string[] = [];
  public write(msg: string): boolean {
    this.writes.push(msg);
    return true;
  }
}

class MockLineageClient {
  public processOpenLineageRunEventCalledWith: any[] = [];
  public processOpenLineageRunEventError: Error | null = null;

  public async processOpenLineageRunEvent(request: any): Promise<any> {
    this.processOpenLineageRunEventCalledWith.push(request);
    if (this.processOpenLineageRunEventError) {
      throw this.processOpenLineageRunEventError;
    }
  }
}

suite("LineageEmitter", () => {
  const credentials = dataform.BigQuery.create({
    projectId: "test-project",
    location: "US"
  });

  test("emits open lineage run event with correct payload", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true, projectDir: "/workspaces/my-dataform-project" },
      () => mockClient as any
    );

    const action = dataform.ExecutionAction.create({
      target: {
        database: "target-project",
        schema: "target_dataset",
        name: "target_table"
      },
      type: "table",
      fileName: "definitions/target_table.sqlx",
      dependencyTargets: [
        {
          database: "source-project",
          schema: "source_dataset",
          name: "source_table"
        }
      ],
      tasks: [
        {
          statement: "CREATE TABLE target_table AS SELECT * FROM source_table"
        }
      ]
    });

    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING,
      timing: { startTimeMillis: Long.fromNumber(1000) }
    });
    emitter.emitForAction(action, startResult);

    const completeResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      timing: {
        startTimeMillis: Long.fromNumber(1000),
        endTimeMillis: Long.fromNumber(2000)
      },
      tasks: [{ status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL }]
    });
    emitter.emitForAction(action, completeResult);

    await emitter.drain();

    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(2);

    const startPayload = mockClient.processOpenLineageRunEventCalledWith[0];
    expect(startPayload.parent).to.equal("projects/target-project/locations/us");
    const startOpenLineage = fromProtoStruct(startPayload.openLineage);
    expect(startOpenLineage.eventType).to.equal("START");
    expect(startOpenLineage.job.name).to.equal(
      "target-project.us.cli.my-dataform-project-0b5d3e86.target_dataset.target_table"
    );

    const completePayload = mockClient.processOpenLineageRunEventCalledWith[1];
    expect(completePayload.parent).to.equal("projects/target-project/locations/us");
    const openLineage = fromProtoStruct(completePayload.openLineage);
    expect(openLineage.run.runId).to.equal(startOpenLineage.run.runId);
    expect(openLineage.eventType).to.equal("COMPLETE");
    expect(openLineage.producer).to.equal("https://github.com/dataform-co/dataform");
    expect(openLineage.job.name).to.equal(
      "target-project.us.cli.my-dataform-project-0b5d3e86.target_dataset.target_table"
    );
    expect(openLineage.inputs[0].namespace).to.equal("bigquery");
    expect(openLineage.inputs[0].name).to.equal("source-project.source_dataset.source_table");
    expect(openLineage.outputs[0].namespace).to.equal("bigquery");
    expect(openLineage.outputs[0].name).to.equal("target-project.target_dataset.target_table");

    expect(openLineage.run.facets.parent.job.name).to.equal(
      "target-project.us.cli.my-dataform-project-0b5d3e86.run"
    );
    expect(openLineage.run.facets.parent.run.runId).to.be.a("string");

    expect(openLineage.run.facets.nominalTime.nominalStartTime).to.equal(
      new Date(1000).toISOString()
    );
    expect(openLineage.run.facets.nominalTime.nominalEndTime).to.equal(
      new Date(2000).toISOString()
    );

    expect(openLineage.job.facets.gcp_lineage.displayName).to.equal(
      "BigQuery Pipelines action target_dataset.target_table"
    );
    expect(openLineage.job.facets.gcp_lineage.origin.sourceType).to.equal("BIGQUERY_PIPELINES");
    expect(openLineage.job.facets.gcp_lineage.origin.name).to.equal(
      "projects/target-project/locations/us/cli/my-dataform-project-0b5d3e86"
    );

    expect(openLineage.run.facets.gcp_bq_pipelines_run.runType).to.equal("cli-manual");
  });

  test("emits FAIL eventType on action failure", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true, projectDir: "/workspaces/my-dataform-project" },
      () => mockClient as any
    );

    const action = dataform.ExecutionAction.create({
      target: {
        database: "target-project",
        schema: "target_dataset",
        name: "failing_table"
      },
      type: "table"
    });

    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING,
      timing: { startTimeMillis: Long.fromNumber(1000) }
    }));
    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.FAILED,
      timing: {
        startTimeMillis: Long.fromNumber(1000),
        endTimeMillis: Long.fromNumber(1500)
      }
    }));

    await emitter.drain();

    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(2);
    const failPayload = mockClient.processOpenLineageRunEventCalledWith[1];
    const openLineage = fromProtoStruct(failPayload.openLineage);
    expect(openLineage.eventType).to.equal("FAIL");
    expect(openLineage.run.facets.nominalTime.nominalEndTime).to.equal(
      new Date(1500).toISOString()
    );
  });

  test("disables emission for the rest of the run after the first failure", async () => {
    const mockClient = new MockLineageClient();
    const err: any = new Error("something broke");
    err.code = 13;
    mockClient.processOpenLineageRunEventError = err;

    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true },
      () => mockClient as any
    );

    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(action, startResult);
    await emitter.drain();
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(1);

    // Subsequent emits must not hit the client.
    for (let i = 0; i < 4; i++) {
      emitter.emitForAction(action, startResult);
    }
    await emitter.drain();
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(1);
  });

  test("skip_reason=dry_run is logged once per run, not once per action", async () => {
    const mockClient = new MockLineageClient();
    const stderr = new StderrCapture();
    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true, dryRun: true },
      () => mockClient as any,
      stderr
    );

    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(action, startResult);
    emitter.emitForAction(action, startResult);
    emitter.emitForAction(action, startResult);
    await emitter.drain();

    const dryRunLines = stderr.writes.filter(w => w.includes("skip_reason=dry_run"));
    expect(dryRunLines.length).to.equal(1);
    expect(dryRunLines[0]).to.contain("dry-run mode");
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(0);
  });

  test("ineligible action types are skipped silently (no stderr line, no emission)", async () => {
    const mockClient = new MockLineageClient();
    const stderr = new StderrCapture();
    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true },
      () => mockClient as any,
      stderr
    );

    const assertion = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "assertion" },
      type: "assertion"
    });
    const declaration = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "declaration" },
      type: "declaration"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(assertion, startResult);
    emitter.emitForAction(declaration, startResult);
    await emitter.drain();

    expect(stderr.writes).to.deep.equal([]);
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(0);
  });

  test("sanitizes projectDir basename with special chars in workdirIdentifier", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true, projectDir: "/workspaces/My Project! v2" },
      () => mockClient as any
    );

    const action = dataform.ExecutionAction.create({
      target: { database: "target-project", schema: "s", name: "t" },
      type: "table"
    });

    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      tasks: [{ status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL }]
    }));
    await emitter.drain();

    const openLineage = fromProtoStruct(
      mockClient.processOpenLineageRunEventCalledWith[0].openLineage
    );
    // "My Project! v2" -> "my-project-v2" + "-" + 8-hex hash slice.
    expect(openLineage.job.facets.gcp_lineage.origin.name).to.match(
      /^projects\/target-project\/locations\/us\/cli\/my-project-v2-[0-9a-f]{8}$/
    );
  });

  test("falls back to 'unknown-workdir' when projectDir is not provided", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true },
      () => mockClient as any
    );

    const action = dataform.ExecutionAction.create({
      target: { database: "target-project", schema: "s", name: "t" },
      type: "table"
    });

    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      tasks: [{ status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL }]
    }));
    await emitter.drain();

    const openLineage = fromProtoStruct(
      mockClient.processOpenLineageRunEventCalledWith[0].openLineage
    );
    expect(openLineage.job.facets.gcp_lineage.origin.name).to.equal(
      "projects/target-project/locations/us/cli/unknown-workdir"
    );
  });

  test("emit failures never surface as unhandled rejections", async () => {
    // Isolation contract: lineage errors must never propagate to the
    // surrounding BQ executor.
    const rejections: unknown[] = [];
    const onUnhandled = (r: unknown) => rejections.push(r);
    process.on("unhandledRejection", onUnhandled);
    try {
      const mockClient = new MockLineageClient();
      const genericError: any = new Error("something broke");
      genericError.code = 13;
      mockClient.processOpenLineageRunEventError = genericError;

      const emitter = new LineageEmitter(
        credentials,
        { lineageEnabled: true },
        () => mockClient as any
      );
      const action = dataform.ExecutionAction.create({
        target: { database: "proj", schema: "s", name: "t" },
        type: "table"
      });
      const startResult = dataform.ActionResult.create({
        status: dataform.ActionResult.ExecutionStatus.RUNNING
      });

      for (let i = 0; i < 5; i++) {
        emitter.emitForAction(action, startResult);
      }
      await emitter.drain();
      await new Promise(r => setImmediate(r));

      expect(rejections).to.deep.equal([]);
    } finally {
      process.removeListener("unhandledRejection", onUnhandled);
    }
  });
});

function fromProtoStruct(struct: any): any {
  if (!struct || !struct.fields) {
    return {};
  }
  const obj: any = {};
  for (const key of Object.keys(struct.fields)) {
    obj[key] = fromProtoValue(struct.fields[key]);
  }
  return obj;
}

function fromProtoValue(value: any): any {
  if (!value) {
    return null;
  }
  if (value.nullValue !== undefined) {
    return null;
  }
  if (value.stringValue !== undefined) {
    return value.stringValue;
  }
  if (value.numberValue !== undefined) {
    return value.numberValue;
  }
  if (value.boolValue !== undefined) {
    return value.boolValue;
  }
  if (value.listValue && value.listValue.values) {
    return value.listValue.values.map(fromProtoValue);
  }
  if (value.structValue) {
    return fromProtoStruct(value.structValue);
  }
  return null;
}
