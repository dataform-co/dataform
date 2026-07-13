import { expect } from "chai";
import Long from "long";

import {
  createLineageClientProvider,
  LINEAGE_RETRY_CONFIG,
  LineageEmitter
} from "df/cli/api/lineage/emitter";
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

  test("emits externalQuery run facet on COMPLETE when a task carries bigquery.jobId", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, () => mockClient as any);

    const action = dataform.ExecutionAction.create({
      target: { database: "target-project", schema: "s", name: "t" },
      type: "table",
      tasks: [{ statement: "SELECT 1" }]
    });

    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    }));
    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      tasks: [
        {
          status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
          metadata: { bigquery: { jobId: "job_abc123" } }
        }
      ]
    }));
    await emitter.drain();

    const startFacets = fromProtoStruct(mockClient.processOpenLineageRunEventCalledWith[0].openLineage).run.facets;
    expect(startFacets.externalQuery).to.equal(undefined);

    const completeFacets = fromProtoStruct(mockClient.processOpenLineageRunEventCalledWith[1].openLineage).run.facets;
    expect(completeFacets.externalQuery.externalQueryId).to.equal("test-project.us.job_abc123");
    expect(completeFacets.externalQuery.source).to.equal("bigquery");
    expect(completeFacets.externalQuery._producer).to.equal("https://github.com/dataform-co/dataform");
  });

  test("emits externalQuery run facet on FAIL when a task carries bigquery.jobId", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, () => mockClient as any);

    const action = dataform.ExecutionAction.create({
      target: { database: "target-project", schema: "s", name: "t" },
      type: "table"
    });

    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.FAILED,
      tasks: [
        {
          status: dataform.TaskResult.ExecutionStatus.FAILED,
          metadata: { bigquery: { jobId: "job_xyz789" } },
          errorMessage: "bigquery error: something"
        }
      ]
    }));
    await emitter.drain();

    const failFacets = fromProtoStruct(mockClient.processOpenLineageRunEventCalledWith[0].openLineage).run.facets;
    expect(failFacets.externalQuery.externalQueryId).to.equal("test-project.us.job_xyz789");
    expect(failFacets.externalQuery.source).to.equal("bigquery");
  });

  test("picks the last non-empty jobId when the action has multiple tasks", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, () => mockClient as any);

    const action = dataform.ExecutionAction.create({
      target: { database: "target-project", schema: "s", name: "t" },
      type: "table"
    });

    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      tasks: [
        { status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL, metadata: { bigquery: { jobId: "job_preop" } } },
        { status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL, metadata: { bigquery: { jobId: "job_main" } } },
        { status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL, metadata: {} }
      ]
    }));
    await emitter.drain();

    const facets = fromProtoStruct(mockClient.processOpenLineageRunEventCalledWith[0].openLineage).run.facets;
    expect(facets.externalQuery.externalQueryId).to.equal("test-project.us.job_main");
  });

  test("omits externalQuery run facet when no task carries bigquery.jobId", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, () => mockClient as any);

    const action = dataform.ExecutionAction.create({
      target: { database: "target-project", schema: "s", name: "t" },
      type: "table"
    });

    emitter.emitForAction(action, dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      tasks: [{ status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL, metadata: {} }]
    }));
    await emitter.drain();

    const facets = fromProtoStruct(mockClient.processOpenLineageRunEventCalledWith[0].openLineage).run.facets;
    expect(facets.externalQuery).to.equal(undefined);
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

  test("LINEAGE_RETRY_CONFIG declares canonical transient set + exponential backoff", () => {
    expect(LINEAGE_RETRY_CONFIG.retryCodes).to.deep.equal([4, 8, 10, 13, 14]);
    expect(LINEAGE_RETRY_CONFIG.backoffSettings).to.deep.include({
      initialRetryDelayMillis: 1000,
      retryDelayMultiplier: 2.0,
      maxRetryDelayMillis: 4000,
      maxRpcTimeoutMillis: 2000,
      totalTimeoutMillis: 15000
    });
  });

  test("non-transient errors propagate without outer-loop retry", async () => {
    const mockClient = new MockLineageClient();
    const invalidArgErr: any = new Error("bad request");
    invalidArgErr.code = 3; // INVALID_ARGUMENT — not in LINEAGE_RETRY_CONFIG.retryCodes
    mockClient.processOpenLineageRunEventError = invalidArgErr;

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

    // Mock bypasses gax, so gax retry never engages. Outer loop makes exactly
    // one call for non-transient errors (no REP fallback, no skip code).
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(1);
  });

  test("creates lineage client provider with custom endpoint", () => {
    const provider = createLineageClientProvider(credentials, "my-custom-endpoint.googleapis.com");
    const client = provider("test-project", "datalineage.us.rep.googleapis.com");
    expect(client).to.not.equal(undefined);
  });

  test("falls back from regional endpoint to global on DNS-unresolvable REP", async () => {
    const mockClient = new MockLineageClient();
    const dnsError: any = new Error("14 UNAVAILABLE: DNS resolution failed for datalineage.us.rep.googleapis.com");
    dnsError.code = 14;
    dnsError.cause = { code: "ENOTFOUND", message: "getaddrinfo ENOTFOUND datalineage.us.rep.googleapis.com" };

    let callCount = 0;
    const endpointsUsed: string[] = [];
    const provider = (projectId: string, endpoint: string) => {
      endpointsUsed.push(endpoint);
      callCount++;
      mockClient.processOpenLineageRunEventError = callCount === 1 ? dnsError : null;
      return mockClient as any;
    };

    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, provider);

    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(action, startResult);
    await emitter.drain();

    expect(endpointsUsed).to.deep.equal([
      "datalineage.us.rep.googleapis.com",
      "datalineage.googleapis.com"
    ]);
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(2);
  });

  test("caches REP-unavailable decision across subsequent emits", async () => {
    const mockClient = new MockLineageClient();
    const dnsError: any = new Error("14 UNAVAILABLE: getaddrinfo ENOTFOUND datalineage.us.rep.googleapis.com");
    dnsError.code = 14;
    dnsError.cause = { code: "ENOTFOUND", message: "getaddrinfo ENOTFOUND" };

    let callCount = 0;
    const endpointsUsed: string[] = [];
    const provider = (projectId: string, endpoint: string) => {
      endpointsUsed.push(endpoint);
      callCount++;
      mockClient.processOpenLineageRunEventError = callCount === 1 ? dnsError : null;
      return mockClient as any;
    };

    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, provider);

    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    // First emit: REP fails with ENOTFOUND, falls back to global.
    emitter.emitForAction(action, startResult);
    await emitter.drain();

    // Second emit: should skip REP entirely and go straight to global.
    emitter.emitForAction(action, startResult);
    await emitter.drain();

    expect(endpointsUsed).to.deep.equal([
      "datalineage.us.rep.googleapis.com",
      "datalineage.googleapis.com",
      "datalineage.googleapis.com"
    ]);
  });

  test("falls back on real gRPC 'Name resolution failed' error shape", async () => {
    // Reproduces the exact error shape emitted by the @grpc/grpc-js DNS resolver
    // when a REP hostname does not exist. This locks in that isEndpointUnresolvable
    // matches the production message (regression: earlier regex only matched
    // 'DNS resolution', missing the gRPC 'Name resolution failed' phrasing).
    const mockClient = new MockLineageClient();
    // Exact shape observed in a live run: google-gax wraps the underlying
    // UNAVAILABLE(14) into a DEADLINE_EXCEEDED(4) after its retry budget
    // expires. The DNS signature is only in the message string.
    const grpcDnsError: any = new Error(
      "Total timeout of API google.cloud.datacatalog.lineage.v1.Lineage exceeded 2000 milliseconds retrying error Error: 14 UNAVAILABLE: Name resolution failed for target dns:datalineage.bogusregion.rep.googleapis.com:443"
    );
    grpcDnsError.code = 4;

    let callCount = 0;
    const endpointsUsed: string[] = [];
    const provider = (projectId: string, endpoint: string) => {
      endpointsUsed.push(endpoint);
      callCount++;
      mockClient.processOpenLineageRunEventError = callCount === 1 ? grpcDnsError : null;
      return mockClient as any;
    };

    const emitter = new LineageEmitter(credentials, { lineageEnabled: true }, provider);

    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "schema", name: "table" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(action, startResult);
    await emitter.drain();

    expect(endpointsUsed).to.deep.equal([
      "datalineage.us.rep.googleapis.com",
      "datalineage.googleapis.com"
    ]);
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

  test("skip_reason=api_disabled is logged when the API returns PERMISSION_DENIED", async () => {
    const mockClient = new MockLineageClient();
    const stderr = new StderrCapture();
    const permissionError: any = new Error("Permission Denied");
    permissionError.code = 7;
    mockClient.processOpenLineageRunEventError = permissionError;

    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true },
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
    await emitter.drain();

    const apiDisabledLines = stderr.writes.filter(w => w.includes("skip_reason=api_disabled"));
    expect(apiDisabledLines.length).to.equal(1);
    expect(apiDisabledLines[0]).to.contain("permission check failed");
  });

  test("skip_reason=api_disabled is logged when the API returns SERVICE_DISABLED", async () => {
    const mockClient = new MockLineageClient();
    const stderr = new StderrCapture();
    const serviceDisabledError: any = new Error("SERVICE_DISABLED: Data Lineage API is disabled");
    serviceDisabledError.code = 9;
    mockClient.processOpenLineageRunEventError = serviceDisabledError;

    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true },
      () => mockClient as any,
      stderr
    );
    const action = dataform.ExecutionAction.create({
      target: { database: "target-proj", schema: "schema", name: "table" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(action, startResult);
    await emitter.drain();

    const apiDisabledLines = stderr.writes.filter(w => w.includes("skip_reason=api_disabled"));
    expect(apiDisabledLines.length).to.equal(1);
    expect(apiDisabledLines[0]).to.contain("gcloud services enable datalineage.googleapis.com");
    expect(apiDisabledLines[0]).to.contain("target-proj");
  });

  test("skip_reason=api_disabled is logged once when many in-flight calls all fail", async () => {
    // Regression: when the first RPC has not yet returned before subsequent
    // emit calls dispatch their own RPCs, the public-method guard cannot
    // short-circuit them. All N rejections then hit the catch. Without a guard
    // inside the catch, N stderr lines are printed. The guard must dedupe.
    const mockClient = new MockLineageClient();
    const stderr = new StderrCapture();
    const permissionError: any = new Error("Permission Denied");
    permissionError.code = 7;
    mockClient.processOpenLineageRunEventError = permissionError;

    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true },
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

    for (let i = 0; i < 5; i++) {
      emitter.emitForAction(action, startResult);
    }
    await emitter.drain();

    const apiDisabledLines = stderr.writes.filter(w => w.includes("skip_reason=api_disabled"));
    expect(apiDisabledLines.length).to.equal(1);
  });

  test("does not fall back to global when apiEndpoint override is set", async () => {
    const mockClient = new MockLineageClient();
    const dnsError: any = new Error("14 UNAVAILABLE: getaddrinfo ENOTFOUND staging-datalineage.sandbox.googleapis.com");
    dnsError.code = 14;
    dnsError.cause = { code: "ENOTFOUND", message: "getaddrinfo ENOTFOUND" };
    mockClient.processOpenLineageRunEventError = dnsError;

    const endpointsUsed: string[] = [];
    const provider = (projectId: string, endpoint: string) => {
      endpointsUsed.push(endpoint);
      return mockClient as any;
    };

    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true, apiEndpoint: "staging-datalineage.sandbox.googleapis.com" },
      provider
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

    // Every attempt used the override; no fallback to global was attempted.
    for (const used of endpointsUsed) {
      expect(used).to.equal("staging-datalineage.sandbox.googleapis.com");
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
