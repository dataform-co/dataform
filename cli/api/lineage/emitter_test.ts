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
    expect(startOpenLineage.job.name).to.equal("target-project.us.cli.my-dataform-project-0b5d3e86.target_dataset.target_table");

    // Assert COMPLETE event
    const completePayload = mockClient.processOpenLineageRunEventCalledWith[1];
    expect(completePayload.parent).to.equal("projects/target-project/locations/us");
    const openLineage = fromProtoStruct(completePayload.openLineage);
    expect(openLineage.run.runId).to.equal(startOpenLineage.run.runId);
    expect(openLineage.eventType).to.equal("COMPLETE");
    expect(openLineage.producer).to.equal("https://github.com/dataform-co/dataform");
    expect(openLineage.job.name).to.equal("target-project.us.cli.my-dataform-project-0b5d3e86.target_dataset.target_table");
    expect(openLineage.inputs[0].namespace).to.equal("bigquery");
    expect(openLineage.inputs[0].name).to.equal("source-project.source_dataset.source_table");
    expect(openLineage.outputs[0].namespace).to.equal("bigquery");
    expect(openLineage.outputs[0].name).to.equal("target-project.target_dataset.target_table");

    // Assert Parent run facet
    expect(openLineage.run.facets.parent.job.name).to.equal("target-project.us.cli.my-dataform-project-0b5d3e86.run");
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
    expect(openLineage.job.facets.gcp_lineage.displayName).to.equal("BigQuery Pipelines action target_dataset.target_table");
    expect(openLineage.job.facets.gcp_lineage.origin.sourceType).to.equal("BIGQUERY_PIPELINES");
    expect(openLineage.job.facets.gcp_lineage.origin.name).to.equal("projects/target-project/locations/us/cli/my-dataform-project-0b5d3e86");

    // Job type facet verified
    expect(openLineage.job.facets.jobType.integration).to.equal("BIGQUERY_PIPELINES");
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
    expect(openLineage.job.name).to.equal("target-project.us.cli.my-dataform-project-0b5d3e86.target_dataset.failing_table");

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

    // Run first action (fails on write, setting emissionDisabledThisRun to true)
    emitter.emitForAction(action, startResult);
    await emitter.drain();

    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(1);

    // Run second action (should skip immediately because emissionDisabledThisRun is true)
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

  test("skip_reason=api_disabled on PERMISSION_DENIED surfaces both IAM and API-enable hints", async () => {
    // PERMISSION_DENIED is ambiguous on GCP — it fires for missing IAM roles
    // AND for "API not enabled" (enablement checks often surface as code 7,
    // not 9). The hint must cover both so users don't chase the wrong cause.
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
    expect(apiDisabledLines[0]).to.contain(
      "datalineage.googleapis.com/locations.processOpenLineageMessage"
    );
    expect(apiDisabledLines[0]).to.contain("gcloud services enable datalineage.googleapis.com");
    expect(apiDisabledLines[0]).to.contain(" OR ");
    expect(apiDisabledLines[0]).to.contain("target-proj");
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

  test("skip_reason=unauthenticated is logged on UNAUTHENTICATED and disables emission for the rest of the run", async () => {
    // UNAUTHENTICATED (code 16) is non-recoverable within a single CLI run —
    // credentials are loaded once at startup. Match the PERMISSION_DENIED /
    // SERVICE_DISABLED shape: flip the kill-switch, print exactly one hint,
    // and suppress the generic [lineage] Failed to emit line for follow-up
    // emits.
    const mockClient = new MockLineageClient();
    const stderr = new StderrCapture();
    const unauthenticatedError: any = new Error("UNAUTHENTICATED: Request had invalid authentication credentials");
    unauthenticatedError.code = 16;
    mockClient.processOpenLineageRunEventError = unauthenticatedError;

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

    // First emit — RPC fires, fails with UNAUTHENTICATED, kill-switch flips.
    emitter.emitForAction(action, startResult);
    await emitter.drain();

    // Second emit — kill-switch is set, so no client call should happen.
    emitter.emitForAction(action, startResult);
    await emitter.drain();

    // Exactly one RPC (kill-switch prevented the second) and exactly one
    // stderr line — the fully-static skip_reason=unauthenticated hint. No
    // generic "[lineage] Failed to emit" line is written.
    expect(mockClient.processOpenLineageRunEventCalledWith.length).to.equal(1);
    expect(stderr.writes).to.deep.equal([
      "[lineage] Skipped lineage emission for the rest of this run: skip_reason=unauthenticated (the credential used to reach the Lineage API is missing, invalid, or expired; re-authenticate and rerun — e.g., 'gcloud auth application-default login')\n"
    ]);
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

  test("sanitizes projectDir basename with special chars in workdirIdentifier", async () => {
    const mockClient = new MockLineageClient();
    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true, projectDir: "/workspaces/My Project! v2" },
      () => mockClient as any
    );

    const action = dataform.ExecutionAction.create({
      target: { database: "target-project", schema: "s", name: "t" },
      type: "table",
      tasks: [{ statement: "SELECT 1" }]
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
      type: "table",
      tasks: [{ statement: "SELECT 1" }]
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

  test("REP endpoint falls back to global on HTTP 302 (region mismatch)", async () => {
    // Symmetric to the DNS-unresolvable fallback: if a REP hostname is
    // reachable but returns 302 (no backend route for the location), we
    // should still fall through to the global endpoint rather than skip.
    const mockClient = new MockLineageClient();
    const redirectError: any = new Error("302:Found");
    redirectError.code = 2;

    let callCount = 0;
    const endpointsUsed: string[] = [];
    const provider = (projectId: string, endpoint: string) => {
      endpointsUsed.push(endpoint);
      callCount++;
      mockClient.processOpenLineageRunEventError = callCount === 1 ? redirectError : null;
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

  test("Failed to emit line carries code name, endpoint, location, and message", async () => {
    // Unhandled error kinds (anything other than DNS-unresolvable, 302,
    // PERMISSION_DENIED, FAILED_PRECONDITION/SERVICE_DISABLED) reach the
    // outer catch. That line is the diagnostic surface for the user — it
    // must name the gRPC code, endpoint, and location so support can triage
    // without asking for the transport log.
    const mockClient = new MockLineageClient();
    const stderr = new StderrCapture();
    const invalidArgErr: any = new Error("bad request");
    invalidArgErr.code = 3; // INVALID_ARGUMENT — not in any handled bucket
    mockClient.processOpenLineageRunEventError = invalidArgErr;

    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true },
      () => mockClient as any,
      stderr
    );
    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "myschema", name: "mytable" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(action, startResult);
    await emitter.drain();

    const failLines = stderr.writes.filter(w => w.includes("[lineage] Failed to emit"));
    expect(failLines.length).to.equal(1);
    expect(failLines[0]).to.contain("myschema.mytable");
    expect(failLines[0]).to.contain("code=INVALID_ARGUMENT(3)");
    expect(failLines[0]).to.contain("endpoint=datalineage.us.rep.googleapis.com");
    expect(failLines[0]).to.contain("location=us");
    expect(failLines[0]).to.contain("message=bad request");
  });

  test("emit failures never surface as unhandled rejections", async () => {
    // Isolation contract: lineage errors must never propagate to the
    // surrounding BQ executor. Regression guard so a future refactor of the
    // .catch/.finally chain (or of drain()) can't silently break it. Uses a
    // non-handled error kind so the promise chain has to catch it explicitly.
    const rejections: unknown[] = [];
    const onUnhandled = (r: unknown) => rejections.push(r);
    process.on("unhandledRejection", onUnhandled);
    try {
      const mockClient = new MockLineageClient();
      const genericError: any = new Error("something broke");
      genericError.code = 13; // INTERNAL — handled by gax retry, then propagates
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
      // Yield one more microtask cycle so any late rejections settle.
      await new Promise(r => setImmediate(r));

      expect(rejections).to.deep.equal([]);
    } finally {
      process.removeListener("unhandledRejection", onUnhandled);
    }
  });

  test("caches REP-unavailable decision after HTTP 302 fallback", async () => {
    // Symmetric to the DNS-unresolvable cache test above: after a 302
    // triggers REP -> global fallback, subsequent emits for the same
    // location must skip REP entirely instead of re-probing it every time.
    const mockClient = new MockLineageClient();
    const redirectError: any = new Error("302:Found");
    redirectError.code = 2;

    let callCount = 0;
    const endpointsUsed: string[] = [];
    const provider = (projectId: string, endpoint: string) => {
      endpointsUsed.push(endpoint);
      callCount++;
      mockClient.processOpenLineageRunEventError = callCount === 1 ? redirectError : null;
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

    emitter.emitForAction(action, startResult);
    await emitter.drain();

    expect(endpointsUsed).to.deep.equal([
      "datalineage.us.rep.googleapis.com",
      "datalineage.googleapis.com",
      "datalineage.googleapis.com"
    ]);
  });

  test("Failed to emit line names the global endpoint after REP fallback also fails", async () => {
    // When REP -> global fallback is triggered but the global attempt also
    // fails with an unhandled error, the structured error line must name
    // the endpoint that was actually attempted last (global), not the
    // original REP hostname. Regression guard: proves the per-attempt
    // metadata on the error object is refreshed for the fallback attempt.
    const mockClient = new MockLineageClient();
    const stderr = new StderrCapture();
    const dnsError: any = new Error(
      "14 UNAVAILABLE: Name resolution failed for target dns:datalineage.us.rep.googleapis.com:443"
    );
    dnsError.code = 4;
    const invalidArgErr: any = new Error("bad request");
    invalidArgErr.code = 3;

    let callCount = 0;
    const provider = (projectId: string, endpoint: string) => {
      callCount++;
      mockClient.processOpenLineageRunEventError = callCount === 1 ? dnsError : invalidArgErr;
      return mockClient as any;
    };

    const emitter = new LineageEmitter(
      credentials,
      { lineageEnabled: true },
      provider,
      stderr
    );
    const action = dataform.ExecutionAction.create({
      target: { database: "proj", schema: "myschema", name: "mytable" },
      type: "table"
    });
    const startResult = dataform.ActionResult.create({
      status: dataform.ActionResult.ExecutionStatus.RUNNING
    });

    emitter.emitForAction(action, startResult);
    await emitter.drain();

    const failLines = stderr.writes.filter(w => w.includes("[lineage] Failed to emit"));
    expect(failLines.length).to.equal(1);
    expect(failLines[0]).to.contain("endpoint=datalineage.googleapis.com");
    expect(failLines[0]).to.not.contain("endpoint=datalineage.us.rep.googleapis.com");
    expect(failLines[0]).to.contain("code=INVALID_ARGUMENT(3)");
  });

  test("non-DNS / non-302 error from REP does not trigger fallback to global", async () => {
    // Only two error shapes trigger REP -> global fallback: DNS-unresolvable
    // and HTTP 302. An INTERNAL from REP means REP itself is reachable and
    // serving; it should propagate to the structured error line without
    // ever calling the global endpoint. Locks in that the fallback
    // conditions can't quietly broaden.
    const mockClient = new MockLineageClient();
    const internalErr: any = new Error("upstream broke");
    internalErr.code = 13;
    mockClient.processOpenLineageRunEventError = internalErr;

    const endpointsUsed: string[] = [];
    const provider = (projectId: string, endpoint: string) => {
      endpointsUsed.push(endpoint);
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

    expect(endpointsUsed).to.deep.equal(["datalineage.us.rep.googleapis.com"]);
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
