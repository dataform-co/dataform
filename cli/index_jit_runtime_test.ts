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

suite("JiT support runtime", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("JiT require() of local files support", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    // Add a helper JS file
    fs.ensureDirSync(path.join(projectDir, "helpers"));
    fs.writeFileSync(
      path.join(projectDir, "helpers", "utils.js"),
      "module.exports = { getValue: () => 'required_value' };"
    );
    // Add a JiT table that requires it
    fs.writeFileSync(
      path.join(projectDir, "definitions", "jit_require_test.js"),
      `publish("jit_require_test", { type: "table" }).jitCode(async (jctx) => {
        const utils = require("../helpers/utils.js");
        return "SELECT '" + utils.getValue() + "' as val";
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
        "--actions=jit_require_test"
      ])
    );

    expect(runResult.exitCode).equals(0);
    const executedGraph = JSON.parse(runResult.stdout);
    const reqAction = executedGraph.actions.find((a: any) => a.target.name === "jit_require_test");
    expect(reqAction.tasks[0].statement).to.include("required_value");
  });

  test("JiT worker timeout handling", { timeout: 15000 }, async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);

    // Add a JiT table that hangs in an infinite loop
    const hangPath = path.join(projectDir, "definitions", "hang_jit.js");
    fs.writeFileSync(
      hangPath,
      `publish("hang_jit", { type: "table" }).jitCode(async (jctx) => {
        while(true) { /* loop */ }
        return "SELECT 1";
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
        "--actions=hang_jit",
        "--timeout=4s"
      ], { timeout: 20000 })
    );

    expect(runResult.exitCode).equals(1);
    expect(runResult.stdout).to.include("Worker timed out");
  });

  test("JiT parallel execution robustness", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    // Add multiple JiT tables
    for (let i = 0; i < 5; i++) {
      fs.writeFileSync(
        path.join(projectDir, "definitions", `jit_${i}.js`),
        `publish("jit_${i}", { type: "table" }).jitCode(async (jctx) => "SELECT ${i} as val")`
      );
    }

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
    expect(executedGraph.actions.filter((a: any) => a.target.name.startsWith("jit_")).length).to.equal(6); // jit_table + 5 others
  });

  test("JiT handles hard worker crash", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    // Add a JiT table that crashes the process
    const crashPath = path.join(projectDir, "definitions", "crash_jit.js");
    fs.writeFileSync(
      crashPath,
      `publish("crash_jit", { type: "table" }).jitCode(async (jctx) => {
        setTimeout(() => { throw new Error("Hard crash"); }, 10);
        return new Promise(() => {}); // Hang until crash
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
        "--actions=crash_jit"
      ])
    );

    expect(runResult.exitCode).equals(1);
    const executedGraph = JSON.parse(runResult.stdout);
    const crashAction = executedGraph.actions.find((a: any) => a.target.name === "crash_jit");
    expect(crashAction.status).to.equal(3); // FAILED
    expect(crashAction.tasks[0].errorMessage).to.include("Worker exited with code 1");
  });
});
