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

suite("JiT support advanced", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("JiT preOps and postOps support", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const prePostPath = path.join(projectDir, "definitions", "pre_post_jit.js");
    fs.writeFileSync(
      prePostPath,
      `publish("pre_post_jit", { type: "table" }).jitCode(async (jctx) => {
        return {
          query: "SELECT 1 as id",
          preOps: ["SELECT 'pre' as p"],
          postOps: ["SELECT 'post' as p"]
        };
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
        "--actions=pre_post_jit"
      ])
    );

    expect(runResult.exitCode).equals(0);
    const executedGraph = JSON.parse(runResult.stdout);
    const prePostAction = executedGraph.actions.find((a: any) => a.target.name === "pre_post_jit");
    const statement = prePostAction.tasks[0].statement;
    expect(statement).to.include("SELECT 'pre' as p");
    expect(statement).to.include("SELECT 1 as id");
    expect(statement).to.include("SELECT 'post' as p");
  });

  test("JiT incremental pre/post ops support", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    const incPrePostPath = path.join(projectDir, "definitions", "inc_pre_post_jit.js");
    fs.writeFileSync(
      incPrePostPath,
      `publish("inc_pre_post_jit", { type: "incremental" }).jitCode(async (jctx) => {
        if (jctx.incremental()) {
          return {
            query: "SELECT 'inc_path_query' as q",
            preOps: ["SELECT 'inc_path_pre' as p"]
          };
        } else {
          return {
            query: "SELECT 'reg_path_query' as q",
            preOps: ["SELECT 'reg_path_pre' as p"]
          };
        }
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
        "--actions=inc_pre_post_jit",
        "--full-refresh"
      ])
    );

    expect(runResult.exitCode).equals(0);
    const executedGraph = JSON.parse(runResult.stdout);
    const incAction = executedGraph.actions.find((a: any) => a.target.name === "inc_pre_post_jit");
    const statement = incAction.tasks[0].statement;
    expect(statement).to.include("SELECT 'reg_path_pre' as p");
    expect(statement).to.include("SELECT 'reg_path_query' as q");
  });

  test("JiT project-level data support", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    fs.writeFileSync(
      path.join(projectDir, "definitions", "project_data.js"),
      "const { session } = require('@dataform/core');\nsession.jitData('app_secret', 'e2e_secret_value');"
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "jit_data_test.js"),
      `publish("jit_data_test", { type: "table" }).jitCode(async (jctx) => {
        const secret = jctx.data.app_secret;
        return "SELECT '" + secret + "' as val";
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
        "--actions=jit_data_test"
      ])
    );

    expect(runResult.exitCode).equals(0);
    const executedGraph = JSON.parse(runResult.stdout);
    const dataAction = executedGraph.actions.find((a: any) => a.target.name === "jit_data_test");
    expect(dataAction.tasks[0].statement).to.include("e2e_secret_value");
  });

  test("JiT complex session data support", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupJitProject(tmpDirFixture, projectDir);
    fs.writeFileSync(
      path.join(projectDir, "definitions", "complex_project_data.js"),
      "const { session } = require('@dataform/core');\n" +
      "session.jitData('app_config', {\n" +
      "  env: 'test-env',\n" +
      "  version: 1.2,\n" +
      "  tags: ['t1', 't2']\n" +
      "});"
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "jit_complex_data_test.js"),
      "publish('jit_complex_data_test', { type: 'table' }).jitCode(async (jctx) => {\n" +
      "  const config = jctx.data.app_config;\n" +
      "  return 'SELECT \\'' + config.env + '\\' as env, ' + config.version + ' as ver, \\'' + config.tags[0] + '\\' as tag';\n" +
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
        "--actions=jit_complex_data_test"
      ])
    );

    expect(runResult.exitCode).equals(0);
    const executedGraph = JSON.parse(runResult.stdout);
    const dataAction = executedGraph.actions.find((a: any) => a.target.name === "jit_complex_data_test");
    expect(dataAction.tasks[0].statement).to.include("SELECT 'test-env' as env, 1.2 as ver, 't1' as tag");
  });
});
