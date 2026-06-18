import { expect } from "chai";
import Long from "long";

import { createLineageClientProvider, LineageEmitter } from "df/cli/api/lineage/emitter";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

class MockLineageClient {
  public listProcessesCalledWith: any[] = [];
  public processOpenLineageRunEventCalledWith: any[] = [];
  public listProcessesError: Error | null = null;
  public processOpenLineageRunEventError: Error | null = null;

  public async listProcesses(request: any): Promise<any> {
    this.listProcessesCalledWith.push(request);
    if (this.listProcessesError) {
      throw this.listProcessesError;
    }
    return [[]];
  }

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

    // 1. Emit START event
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING,
      timing: {
        startTimeMillis: Long.fromNumber(1000)
      }
    });
    emitter.emitForAction(action, startResult);

    // 2. Emit COMPLETE event
    const completeResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      timing: {
        startTimeMillis: Long.fromNumber(1000),
        endTimeMillis: Long.fromNumber(2000)
      },
      tasks: [
        {
          status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL
        }
      ]
    });
    emitter.emitForAction(action, completeResult);

    // Wait for all background emissions to drain
    await emitter.drain();

    // No preflight was called (no listProcesses)
    expect(mockClient.listProcessesCalledWith.length).to.equal(0);

    // Two events were emitted
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(2);

    // Assert START event
    const startPayload = mockClient.processOpenLineageRunEventCalledWith[0];
    expect(startPayload.parent).to.equal("projects/target-project/locations/us");
    const startOpenLineage = fromProtoStruct(startPayload.openLineage);
    expect(startOpenLineage.eventType).to.equal("START");

    // Assert COMPLETE event
    const completePayload = mockClient.processOpenLineageRunEventCalledWith[1];
    expect(completePayload.parent).to.equal("projects/target-project/locations/us");
    const openLineage = fromProtoStruct(completePayload.openLineage);
    expect(openLineage.run.runId).to.equal(startOpenLineage.run.runId);
    expect(openLineage.eventType).to.equal("COMPLETE");
    expect(openLineage.producer).to.equal("https://github.com/dataform-co/dataform");
    expect(openLineage.inputs[0].namespace).to.equal("bigquery");
    expect(openLineage.inputs[0].name).to.equal("source-project.source_dataset.source_table");
    expect(openLineage.outputs[0].namespace).to.equal("bigquery");
    expect(openLineage.outputs[0].name).to.equal("target-project.target_dataset.target_table");

    // Nominal time run facet verified
    expect(openLineage.run.facets.nominalTime.nominalStartTime).to.equal(
      new Date(1000).toISOString()
    );

    // SQL job facet verified
    expect(openLineage.job.facets.sql.query).to.equal(
      "CREATE TABLE target_table AS SELECT * FROM source_table"
    );

    // Source code location job facet verified
    expect(openLineage.job.facets.sourceCodeLocation.url).to.equal("definitions/target_table.sqlx");
  });

  test("handles permission denied error by disabling emission on subsequent calls", async () => {
    const mockClient = new MockLineageClient();
    const permissionError: any = new Error("Permission Denied");
    permissionError.code = 7;
    mockClient.processOpenLineageRunEventError = permissionError;

    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, () => mockClient as any);

    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    // Run first action (fails on write, setting apiDisabledThisRun to true)
    emitter.emitForAction(action, startResult);
    await emitter.drain();

    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(1);

    // Run second action (should skip immediately because apiDisabledThisRun is true)
    emitter.emitForAction(action, startResult);
    await emitter.drain();

    // Still only 1 call total
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(1);
  });

  test("handles transient errors with retry", async () => {
    const mockClient = new MockLineageClient();
    const transientError: any = new Error("Service Unavailable");
    transientError.code = 14;
    mockClient.processOpenLineageRunEventError = transientError;

    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, () => mockClient as any);

    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(action, startResult);
    await emitter.drain();

    // Retries once (attempts = 1, then attempts = 2), so 2 total calls before swallowing
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(2);
  });

  test("creates lineage client provider with custom endpoint", () => {
    const provider = createLineageClientProvider(credentials, "my-custom-endpoint.googleapis.com");
    const client = provider("test-project");
    expect(client).to.not.equal(undefined);
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
