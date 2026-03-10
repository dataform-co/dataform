import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { execFile } from "child_process";
import {
  cliEntryPointPath,
  CREDENTIALS_PATH,
  setupJitProject
} from "df/cli/index_test_base";
import { getProcessResult, nodePath, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("JiT support main", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("compile command includes jitCode in output", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const compileResult = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--json"])
    );

    if (compileResult.exitCode !== 0) {
      // tslint:disable-next-line:no-console
      console.error("COMPILE FAILED:", compileResult.stderr);
    }
    expect(compileResult.exitCode).equals(0);
    const compiledGraph = JSON.parse(compileResult.stdout);
    const jitTable = compiledGraph.tables.find((t: any) => t.target.name === "jit_table");
    expect(!!jitTable).to.equal(true);
    expect(jitTable.type).to.equal("table");
    expect(jitTable.jitCode).to.contain("async (ctx) => { return \"SELECT 1 as id\"; }");
    expect(compiledGraph).to.have.property("jitData");
  });

  test("fails if both query and jitCode are provided", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const conflictPath = path.join(projectDir, "definitions", "conflict.js");
    fs.writeFileSync(
      conflictPath,
      `publish("conflict", {type: "table"}).query("SELECT 1").jitCode(async (ctx) => "SELECT 2")`
    );

    const compileResult = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--json"])
    );

    expect(compileResult.exitCode).equals(1);
    expect(compileResult.stderr).to.include("Cannot mix AoT and JiT compilation in action");
  });

  test("run command performs JiT compilation during execution", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);

    const runResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "run",
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--actions=jit_table"
      ])
    );

    expect(runResult.exitCode).equals(0);

    const executedGraph = JSON.parse(runResult.stdout);
    const jitAction = executedGraph.actions.find((a: any) => a.target.name === "jit_table");
    expect(!!jitAction).to.equal(true);
    // Tasks array should be populated by the JiT runner
    expect(jitAction.tasks.length).to.be.greaterThan(0);
    expect(jitAction.tasks[0].statement).to.include("SELECT 1 as id");
  });

  test("mixed AoT and JiT support", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const aotTablePath = path.join(projectDir, "definitions", "aot_table.sqlx");
    fs.writeFileSync(aotTablePath, "config { type: 'table' } SELECT 2 as id");

    const runResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "run",
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json"
      ])
    );

    expect(runResult.exitCode).equals(0);

    const executedGraph = JSON.parse(runResult.stdout);
    const aotAction = executedGraph.actions.find((a: any) => a.target.name === "aot_table");
    const jitAction = executedGraph.actions.find((a: any) => a.target.name === "jit_table");

    expect(!!aotAction).to.equal(true);
    expect(!!jitAction).to.equal(true);
    expect(executedGraph.actions.length).to.equal(2);

    expect(aotAction.tasks[0].statement).to.include("SELECT 2 as id");
    // JiT action should have its tasks populated dynamically
    expect(jitAction.tasks.length).to.be.greaterThan(0);
    expect(jitAction.tasks[0].statement).to.include("SELECT 1 as id");
  });

  test("JiT respects disabled flag", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);

    const disabledPath = path.join(projectDir, "definitions", "disabled_jit.js");
    fs.writeFileSync(
      disabledPath,
      `publish("disabled_jit", { type: "table", disabled: true }).jitCode(async (jctx) => {
         throw new Error("Should not be executed");
       })`
    );

    const runResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "run",
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--json",
        "--actions=disabled_jit"
      ])
    );

    // It should exit successfully.
    expect(runResult.exitCode).equals(0);
    // If no actions are run, stdout might be empty or empty JSON.
    const stdout = runResult.stdout.trim();
    if (stdout.length > 0) {
      const executedGraph = JSON.parse(stdout);
      expect(executedGraph.actions.length).to.equal(0);
    }
  });

  test("JiT compilation failure reporting", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const failingJitPath = path.join(projectDir, "definitions", "failing_jit.js");
    fs.writeFileSync(
      failingJitPath,
      `publish("failing_jit", {type: "table"}).jitCode(async (ctx) => { throw new Error("JiT compilation failed!"); })`
    );

    const runResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "run",
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--actions=failing_jit"
      ])
    );

    expect(runResult.exitCode).equals(1);

    const executedGraph = JSON.parse(runResult.stdout);
    const failingAction = executedGraph.actions.find((a: any) => a.target.name === "failing_jit");

    expect(!!failingAction).to.equal(true);
    expect(failingAction.status).to.equal(3); // FAILED
    expect(failingAction.tasks[0].status).to.equal(3); // FAILED
    expect(failingAction.tasks[0].errorMessage).to.include("JiT compilation failed!");
  });

  test("RPC error propagation during run", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const rpcJitPath = path.join(projectDir, "definitions", "rpc_jit.js");
    fs.writeFileSync(
      rpcJitPath,
      `publish("rpc_jit", {type: "table"}).jitCode(async (jctx) => {
         const table = await jctx.adapter.getTable({target: {database: "db", schema: "sch", name: "tab"}});
         return "SELECT 1 as id";
       })`
    );

    const runResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "run",
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--actions=rpc_jit"
      ])
    );

    expect(runResult.exitCode).equals(1);

    const executedGraph = JSON.parse(runResult.stdout);
    const rpcAction = executedGraph.actions.find((a: any) => a.target.name === "rpc_jit");

    expect(!!rpcAction).to.equal(true);
    expect(rpcAction.status).to.equal(3); // FAILED
    expect(rpcAction.tasks[0].errorMessage).to.include("JiT compilation error");
  });

  test("mixed support with AoT filtered out", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const aotTablePath = path.join(projectDir, "definitions", "aot_table.sqlx");
    fs.writeFileSync(aotTablePath, "config { type: 'table' } SELECT 2 as id");

    const runResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "run",
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--actions=jit_table"
      ])
    );

    expect(runResult.exitCode).equals(0);

    const executedGraph = JSON.parse(runResult.stdout);
    expect(executedGraph.actions.length).to.equal(1);
    const jitAction = executedGraph.actions.find((a: any) => a.target.name === "jit_table");
    expect(!!jitAction).to.equal(true);
    expect(jitAction.tasks.length).to.be.greaterThan(0);
    expect(jitAction.tasks[0].statement).to.include("SELECT 1 as id");
  });

  test("mixed support with JiT filtered out", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const aotTablePath = path.join(projectDir, "definitions", "aot_table.sqlx");
    fs.writeFileSync(aotTablePath, "config { type: 'table' } SELECT 2 as id");

    const runResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "run",
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--actions=aot_table"
      ])
    );

    expect(runResult.exitCode).equals(0);

    const executedGraph = JSON.parse(runResult.stdout);
    expect(executedGraph.actions.length).to.equal(1);
    const aotAction = executedGraph.actions.find((a: any) => a.target.name === "aot_table");
    expect(!!aotAction).to.equal(true);
    expect(aotAction.tasks[0].statement).to.include("SELECT 2 as id");
  });
});
