import { expect } from "chai";

import { run } from "df/cli/api/commands/run";
import { IDbAdapter, IDbClient, IExecutionResult, IExecutionResultRaw } from "df/cli/api/dbadapters";
import { QueryOrAction } from "df/cli/api/dbadapters/execution_sql";
import { createLineageEmitter, ILineageEmitterFactoryInput } from "df/cli/api/lineage/emitter_factory";
import { recorderProvider, RecordingLineageClient } from "df/cli/api/lineage/testing/mock_lineage_client";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

/**
 * Integration coverage for the CLI --emit-lineage path. Drives `createLineageEmitter`
 * + `run()` in-process against a fake IDbAdapter and a RecordingLineageClient.
 * Assertions target recorder.calls, recorder.providerCalls, and the presence of
 * `skip_reason=...` markers in stderr — never the wording of debug lines. See
 * task_T37_impl_assertion_strategy.md for the anti-checklist.
 */

class StderrCapture {
  public writes: string[] = [];
  public write(msg: string): boolean {
    this.writes.push(msg);
    return true;
  }
  public contains(needle: string): boolean {
    return this.writes.some(w => w.includes(needle));
  }
}

/**
 * Minimal IDbAdapter fake: unconditionally successful execute(), no-op schema
 * lifecycle, no metadata calls. The Runner's contract with the adapter is
 * exercised end-to-end without touching BigQuery.
 */
class FakeDbAdapter implements IDbAdapter {
  public executed: string[] = [];

  public async execute(statement: string): Promise<IExecutionResult> {
    this.executed.push(statement);
    return { rows: [], metadata: {} };
  }
  public async executeRaw(statement: string): Promise<IExecutionResultRaw> {
    return { rows: [], metadata: {} };
  }
  public async withClientLock<T>(cb: (client: IDbClient) => Promise<T>): Promise<T> {
    return cb(this);
  }
  public async evaluate(queryOrAction: QueryOrAction): Promise<dataform.IQueryEvaluation[]> {
    return [];
  }
  public async schemas(database: string): Promise<string[]> {
    return [];
  }
  public async createSchema(database: string, schema: string): Promise<void> {
    return;
  }
  public async tables(database: string, schema?: string): Promise<dataform.ITableMetadata[]> {
    return [];
  }
  public async search(): Promise<dataform.ITableMetadata[]> {
    return [];
  }
  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    return null;
  }
  public async deleteTable(target: dataform.ITarget): Promise<void> {
    return;
  }
  public async setMetadata(action: dataform.IExecutionAction): Promise<void> {
    return;
  }
}

function makeLinearGraph(): dataform.IExecutionGraph {
  return dataform.ExecutionGraph.create({
    projectConfig: { warehouse: "bigquery", defaultDatabase: "test-project", defaultLocation: "US" },
    runConfig: {},
    warehouseState: { tables: [] },
    actions: [
      {
        target: { database: "test-project", schema: "s", name: "table_a" },
        type: "table",
        tasks: [{ type: "statement", statement: "CREATE TABLE test-project.s.table_a AS SELECT 1" }],
        dependencyTargets: []
      },
      {
        target: { database: "test-project", schema: "s", name: "table_b" },
        type: "table",
        tasks: [{ type: "statement", statement: "CREATE TABLE test-project.s.table_b AS SELECT * FROM table_a" }],
        dependencyTargets: [{ database: "test-project", schema: "s", name: "table_a" }]
      }
    ]
  });
}

function factoryInput(overrides: Partial<ILineageEmitterFactoryInput> = {}): ILineageEmitterFactoryInput {
  return {
    cliEmitLineage: undefined,
    workflowLineageEnabled: undefined,
    workflowApiEndpoint: undefined,
    dryRun: false,
    projectDir: "/workspaces/test-project",
    readCredentials: dataform.BigQuery.create({ projectId: "test-project", location: "US" }),
    ...overrides
  };
}

suite("--emit-lineage integration", { parallel: true }, () => {
  test("Case 1 — CLI flag alone enables emission (2*N recorder calls, pairing invariant)", async () => {
    const recorder = new RecordingLineageClient();
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      factoryInput({ cliEmitLineage: true }),
      stderr,
      recorderProvider(recorder)
    );
    expect(emitter).to.not.equal(undefined);

    const dbadapter = new FakeDbAdapter();
    const runner = run(dbadapter, makeLinearGraph(), { lineageEmitter: emitter });
    await runner.result();
    await emitter!.drain();

    // 2 actions × (START + COMPLETE)
    expect(recorder.calls.length).to.equal(4);

    const runIdsPerTable = new Map<string, Set<string>>();
    const eventTypesPerRunId = new Map<string, string[]>();
    for (const call of recorder.calls) {
      const ol = fromProtoStruct(call.request.openLineage);
      const jobName = ol.job.name as string;
      const runId = ol.run.runId as string;
      const eventType = ol.eventType as string;
      if (!runIdsPerTable.has(jobName)) {
        runIdsPerTable.set(jobName, new Set());
      }
      runIdsPerTable.get(jobName).add(runId);
      if (!eventTypesPerRunId.has(runId)) {
        eventTypesPerRunId.set(runId, []);
      }
      eventTypesPerRunId.get(runId).push(eventType);
    }

    // Every action's START and COMPLETE share exactly one runId.
    expect(runIdsPerTable.size).to.equal(2);
    for (const runIds of runIdsPerTable.values()) {
      expect(runIds.size).to.equal(1);
    }
    // Every runId has exactly one START and one COMPLETE.
    for (const events of eventTypesPerRunId.values()) {
      expect(events.sort()).to.deep.equal(["COMPLETE", "START"]);
    }
  });

  test("Case 2 — workflow lineage.enabled=true (no CLI flag) enables emission", async () => {
    const recorder = new RecordingLineageClient();
    const emitter = createLineageEmitter(
      factoryInput({ workflowLineageEnabled: true }),
      new StderrCapture(),
      recorderProvider(recorder)
    );
    expect(emitter).to.not.equal(undefined);

    const runner = run(new FakeDbAdapter(), makeLinearGraph(), { lineageEmitter: emitter });
    await runner.result();
    await emitter!.drain();

    expect(recorder.calls.length).to.equal(4);
  });

  test("Case 3 — neither flag nor config produces no emission and no factory-provider call", async () => {
    const recorder = new RecordingLineageClient();
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(factoryInput(), stderr, recorderProvider(recorder));

    expect(emitter).to.equal(undefined);
    expect(recorder.calls.length).to.equal(0);
    expect(recorder.providerCalls.length).to.equal(0);
    // Silent default — no skip line either.
    expect(stderr.contains("skip_reason=")).to.equal(false);
  });

  test("Case 4 — CLI --emit-lineage=false overrides workflow lineage.enabled=true", async () => {
    const recorder = new RecordingLineageClient();
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      factoryInput({ cliEmitLineage: false, workflowLineageEnabled: true }),
      stderr,
      recorderProvider(recorder)
    );

    expect(emitter).to.equal(undefined);
    expect(recorder.calls.length).to.equal(0);
    // Precedence surface: CLI-driven override emits a skip_reason line.
    expect(stderr.contains("skip_reason=invocation_override")).to.equal(true);
  });

  test("Case 5 — workflow apiEndpoint override is passed to the client provider", async () => {
    const recorder = new RecordingLineageClient();
    const emitter = createLineageEmitter(
      factoryInput({
        cliEmitLineage: true,
        workflowApiEndpoint: "staging-datalineage.sandbox.googleapis.com"
      }),
      new StderrCapture(),
      recorderProvider(recorder)
    );
    expect(emitter).to.not.equal(undefined);

    const runner = run(new FakeDbAdapter(), makeLinearGraph(), { lineageEmitter: emitter });
    await runner.result();
    await emitter!.drain();

    expect(recorder.providerCalls.length).to.be.greaterThan(0);
    for (const providerCall of recorder.providerCalls) {
      expect(providerCall.endpoint).to.equal("staging-datalineage.sandbox.googleapis.com");
    }
  });

  test("Case 6 — PERMISSION_DENIED on emit is fail-open (run exits SUCCESSFUL, skip_reason logged once)", async () => {
    const recorder = new RecordingLineageClient();
    recorder.throwOnNextCallWith(7, "Permission Denied");
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      factoryInput({ cliEmitLineage: true }),
      stderr,
      recorderProvider(recorder)
    );

    const dbadapter = new FakeDbAdapter();
    const runner = run(dbadapter, makeLinearGraph(), { lineageEmitter: emitter });
    const runResult = await runner.result();
    await emitter!.drain();

    // Workflow itself is unaffected by the lineage failure.
    expect(runResult.status).to.equal(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    expect(dbadapter.executed.length).to.equal(2);

    // T71 once-per-run contract: skip_reason=api_disabled appears exactly once.
    const apiDisabled = stderr.writes.filter(w => w.includes("skip_reason=api_disabled"));
    expect(apiDisabled.length).to.equal(1);
  });

  test("Case 7 — drain is bounded when the client hangs indefinitely", async () => {
    const recorder = new RecordingLineageClient();
    recorder.hangForever();
    const emitter = createLineageEmitter(
      factoryInput({ cliEmitLineage: true }),
      new StderrCapture(),
      recorderProvider(recorder)
    );

    const runner = run(new FakeDbAdapter(), makeLinearGraph(), { lineageEmitter: emitter });
    await runner.result();

    const started = Date.now();
    await emitter!.drain(200);
    const elapsed = Date.now() - started;
    // 200ms bound + generous slack for CI jitter.
    expect(elapsed).to.be.lessThan(2000);
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
