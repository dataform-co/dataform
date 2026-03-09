import { expect } from "chai";
import { execFile } from "child_process";
import * as fs from "fs-extra";
import * as path from "path";

import {
  cliEntryPointPath,
  CREDENTIALS_PATH,
  setupJitProject
} from "df/cli/index_test_base";
import { getProcessResult, nodePath, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("JiT support dependencies", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("JiT transitive dependency pruning", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    // A (AoT) -> B (JiT)
    fs.writeFileSync(
      path.join(projectDir, "definitions", "table_a.sqlx"),
      "config { type: 'table' } SELECT 1 as val"
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "table_b.js"),
      `publish("table_b", { type: "table", dependencies: ["table_a"] }).jitCode(async (jctx) => {
        const upstream = jctx.ref("table_a");
        return "SELECT '" + upstream + "' as ref_name";
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
        "--actions=table_b",
        "--include-deps"
      ])
    );

    expect(runResult.exitCode).equals(0);
    const executedGraph = JSON.parse(runResult.stdout);
    // Should have BOTH tables because of --include-deps
    expect(executedGraph.actions.length).to.equal(2);
    expect(executedGraph.actions.some((a: any) => a.target.name === "table_a")).to.equal(true);
    const actionB = executedGraph.actions.find((a: any) => a.target.name === "table_b");
    expect(actionB).to.not.equal(undefined);
    expect(actionB.tasks[0].statement).to.include("SELECT '`dataform-open-source.dataform.table_a`' as ref_name");
  });

  test("JiT to JiT dependency chain", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    // Action A (JiT) -> Action B (JiT)
    fs.writeFileSync(
      path.join(projectDir, "definitions", "jit_a.js"),
      'publish("jit_a", { type: "table" }).jitCode(async () => "SELECT 1 as val")'
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "jit_b.js"),
      "publish('jit_b', { type: 'table', dependencies: ['jit_a'] }).jitCode(async (jctx) => {\n" +
      "  const upstream = jctx.ref('jit_a');\n" +
      "  return 'SELECT \\'' + upstream + '\\' as ref_name';\n" +
      "})"
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
        "--actions=jit_b",
        "--include-deps"
      ])
    );

    expect(runResult.exitCode).equals(0);
    const executedGraph = JSON.parse(runResult.stdout);
    expect(executedGraph.actions.length).to.equal(2);
    const actionB = executedGraph.actions.find((a: any) => a.target.name === "jit_b");
    expect(actionB.tasks[0].statement).to.include("SELECT '`dataform-open-source.dataform.jit_a`' as ref_name");
  });
});
