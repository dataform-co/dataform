import { expect } from "chai";

import { LineageEmitter } from "df/cli/api/lineage/emitter";
import {
  createLineageEmitter,
  ILineageEmitterFactoryInput
} from "df/cli/api/lineage/emitter_factory";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

class StderrCapture {
  public writes: string[] = [];
  public write(msg: string): boolean {
    this.writes.push(msg);
    return true;
  }
  public get combined(): string {
    return this.writes.join("");
  }
}

const credentials = dataform.BigQuery.create({
  projectId: "test-project",
  location: "US"
});

function baseInput(overrides: Partial<ILineageEmitterFactoryInput> = {}): ILineageEmitterFactoryInput {
  return {
    cliEmitLineage: undefined,
    workflowLineageEnabled: undefined,
    workflowApiEndpoint: undefined,
    dryRun: false,
    projectDir: "/workspaces/my-project",
    readCredentials: credentials,
    ...overrides
  };
}

suite("createLineageEmitter", () => {
  test("skip_reason=workflow_opt_out is logged once when workflow_settings opts out and no CLI flag was passed", () => {
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      baseInput({ workflowLineageEnabled: false }),
      stderr
    );
    expect(emitter).to.equal(undefined);
    expect(stderr.writes.length).to.equal(1);
    expect(stderr.combined).to.contain("skip_reason=workflow_opt_out");
    expect(stderr.combined).to.contain("workflow_settings.yaml lineage.enabled=false");
  });

  test("skip_reason=invocation_override is logged once when --emit-lineage=false was passed", () => {
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      baseInput({ cliEmitLineage: false, workflowLineageEnabled: true }),
      stderr
    );
    expect(emitter).to.equal(undefined);
    expect(stderr.writes.length).to.equal(1);
    expect(stderr.combined).to.contain("skip_reason=invocation_override");
    expect(stderr.combined).to.contain("--emit-lineage=false");
  });

  test("skip_reason=invocation_override wins even when workflow_settings also opts out", () => {
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      baseInput({ cliEmitLineage: false, workflowLineageEnabled: false }),
      stderr
    );
    expect(emitter).to.equal(undefined);
    expect(stderr.combined).to.contain("skip_reason=invocation_override");
    expect(stderr.combined).to.not.contain("skip_reason=workflow_opt_out");
  });

  test("stays silent when neither surface has been touched (opt-in is the default)", () => {
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(baseInput(), stderr);
    expect(emitter).to.equal(undefined);
    expect(stderr.writes).to.deep.equal([]);
  });

  test("constructs a LineageEmitter and stays silent when CLI flag opts in", () => {
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      baseInput({ cliEmitLineage: true }),
      stderr
    );
    expect(emitter).to.be.instanceOf(LineageEmitter);
    expect(stderr.writes).to.deep.equal([]);
  });

  test("constructs a LineageEmitter and stays silent when workflow_settings opts in", () => {
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      baseInput({ workflowLineageEnabled: true }),
      stderr
    );
    expect(emitter).to.be.instanceOf(LineageEmitter);
    expect(stderr.writes).to.deep.equal([]);
  });

  test("returns undefined without logging when credentials are missing", () => {
    const stderr = new StderrCapture();
    const emitter = createLineageEmitter(
      baseInput({ cliEmitLineage: true, readCredentials: undefined }),
      stderr
    );
    expect(emitter).to.equal(undefined);
    expect(stderr.writes).to.deep.equal([]);
  });
});
