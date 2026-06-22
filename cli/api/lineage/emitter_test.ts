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
    expect(startOpenLineage.job.name).to.equal("target-project.us.cli.0b5d3e86239e91e3.target_dataset.target_table");

    // Assert COMPLETE event
    const completePayload = mockClient.processOpenLineageRunEventCalledWith[1];
    expect(completePayload.parent).to.equal("projects/target-project/locations/us");
    const openLineage = fromProtoStruct(completePayload.openLineage);
    expect(openLineage.run.runId).to.equal(startOpenLineage.run.runId);
    expect(openLineage.eventType).to.equal("COMPLETE");
    expect(openLineage.producer).to.equal("https://github.com/dataform-co/dataform");
    expect(openLineage.job.name).to.equal("target-project.us.cli.0b5d3e86239e91e3.target_dataset.target_table");
    expect(openLineage.inputs[0].namespace).to.equal("bigquery");
    expect(openLineage.inputs[0].name).to.equal("source-project.source_dataset.source_table");
    expect(openLineage.outputs[0].namespace).to.equal("bigquery");
    expect(openLineage.outputs[0].name).to.equal("target-project.target_dataset.target_table");

    // Assert Parent run facet
    expect(openLineage.run.facets.parent.job.name).to.equal("target-project.us.cli.0b5d3e86239e91e3.run");
    expect(openLineage.run.facets.parent.run.runId).to.be.a("string");

    // Nominal time run facet verified
    expect(openLineage.run.facets.nominalTime.nominalStartTime).to.equal(
      new Date(1000).toISOString()
    );
    expect(openLineage.run.facets.nominalTime.nominalEndTime).to.equal(
      new Date(2000).toISOString()
    );

    // SQL job facet verified
    expect(openLineage.job.facets.sql.query).to.equal(
      "CREATE TABLE target_table AS SELECT * FROM source_table"
    );

    // Source code location job facet verified
    expect(openLineage.job.facets.sourceCodeLocation.url).to.equal("definitions/target_table.sqlx");

    // GCP lineage job facet verified
    expect(openLineage.job.facets.gcp_lineage.displayName).to.equal("BQ Pipelines action target_dataset.target_table");
    expect(openLineage.job.facets.gcp_lineage.origin.sourceType).to.equal("BQ_PIPELINES");
    expect(openLineage.job.facets.gcp_lineage.origin.name).to.equal("projects/target-project/locations/us/cli/0b5d3e86239e91e3");

    // Job type facet verified
    expect(openLineage.job.facets.jobType.integration).to.equal("BQ_PIPELINES");
    expect(openLineage.job.facets.jobType.jobType).to.equal("ACTION");
    expect(openLineage.job.facets.jobType.processingType).to.equal("BATCH");
  });

  test("emits open lineage run event with correct payload on action failure", async () => {
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
      type: "table",
      fileName: "definitions/failing_table.sqlx"
    });

    // 1. Emit START event
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING,
      timing: {
        startTimeMillis: Long.fromNumber(1000)
      }
    });
    emitter.emitForAction(action, startResult);

    // 2. Emit FAIL event with error message
    const failResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.FAILED,
      timing: {
        startTimeMillis: Long.fromNumber(1000),
        endTimeMillis: Long.fromNumber(1500)
      },
      tasks: [
        {
          status: dataform.TaskResult.ExecutionStatus.FAILED,
          errorMessage: "bigquery error: Syntax error: Unexpected \"\\\" at [3:15]"
        }
      ]
    });
    emitter.emitForAction(action, failResult);

    // Wait for background emissions to drain
    await emitter.drain();

    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(2);

    // Assert FAIL event payload
    const failPayload = mockClient.processOpenLineageRunEventCalledWith[1];
    expect(failPayload.parent).to.equal("projects/target-project/locations/us");
    
    const openLineage = fromProtoStruct(failPayload.openLineage);
    expect(openLineage.eventType).to.equal("FAIL");
    expect(openLineage.job.name).to.equal("target-project.us.cli.0b5d3e86239e91e3.target_dataset.failing_table");

    // Error message run facet verified
    expect(openLineage.run.facets.errorMessage.message).to.equal(
      "bigquery error: Syntax error: Unexpected \"\\\" at [3:15]"
    );
    expect(openLineage.run.facets.errorMessage.programmingLanguage).to.equal("typescript");

    // Nominal time run facet verified (nominalEndTime matches failure timing)
    expect(openLineage.run.facets.nominalTime.nominalStartTime).to.equal(
      new Date(1000).toISOString()
    );
    expect(openLineage.run.facets.nominalTime.nominalEndTime).to.equal(
      new Date(1500).toISOString()
    );
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
