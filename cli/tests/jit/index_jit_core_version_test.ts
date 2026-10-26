import { expect } from "chai";

import {
  alterWorkflowSettings,
  CREDENTIALS_PATH,
  INTEGRATION_TEST_LOCATION,
  INTEGRATION_TEST_PROJECT,
  runCli,
  runCliWithTmpDir,
  setupProject,
} from "df/cli/index_test_base";
import { version } from "df/core/version";
import { test, writeDefinitionFile } from "df/testing";
import { integrationSuite } from "df/testing/credentials";
import { TmpDirFixture } from "df/testing/fixtures";

// A published @dataform/core with JiT support that differs from the core bundled with the CLI, so
// that tests can tell which one JiT compilation used.
const PINNED_CORE_VERSION = "3.0.71";

// The JiT code reports the @dataform/core version it runs with.
const CORE_VERSION_JIT_TABLE = `publish("jit_table", { type: "table" }).jitCode(async (ctx) => {
  const coreVersion = require("@dataform/core").version;
  return \`SELECT '\${coreVersion}' AS core_version\`;
});`;

integrationSuite("JiT with pinned @dataform/core", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  async function setupDataformCoreVersionProject(definition: string): Promise<string> {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await runCli("init", [projectDir, INTEGRATION_TEST_PROJECT, INTEGRATION_TEST_LOCATION]);
    alterWorkflowSettings(projectDir, { dataformCoreVersion: PINNED_CORE_VERSION });
    writeDefinitionFile(projectDir, "jit_table.js", definition);
    return projectDir;
  }

  function runProject(projectDir: string, options: string[]) {
    return runCliWithTmpDir(tmpDirFixture, "run", [
      projectDir,
      "--credentials",
      CREDENTIALS_PATH,
      ...options,
    ]);
  }

  // Dry runs the `jit_table` action and returns the SQL produced by JiT compilation.
  async function dryRunJitTable(projectDir: string): Promise<string> {
    const result = await runProject(projectDir, ["--dry-run", "--json", "--actions=jit_table"]);
    expect(result.exitCode, result.stderr).equals(0);
    expect(result.leftoverEnvironments).deep.equals([]);
    const jitAction = JSON.parse(result.stdout).actions.find(
      (action: any) => action.target.name === "jit_table",
    );
    return jitAction.tasks[0].compiledSql;
  }

  test(
    "dry run uses the core version pinned by dataformCoreVersion",
    { timeout: 120000 },
    async () => {
      const projectDir = await setupDataformCoreVersionProject(CORE_VERSION_JIT_TABLE);

      expect(await dryRunJitTable(projectDir)).to.include(
        `SELECT '${PINNED_CORE_VERSION}' AS core_version`,
      );
    },
  );

  test("run executes JiT actions with dataformCoreVersion", { timeout: 120000 }, async () => {
    const projectDir = await setupDataformCoreVersionProject(
      `operate("jit_operation").jitCode(async (ctx) => "SELECT 1");`,
    );

    const result = await runProject(projectDir, ["--actions=jit_operation"]);

    expect(result.exitCode, `${result.stdout}\n${result.stderr}`).equals(0);
    expect(`${result.stdout}\n${result.stderr}`).to.not.include("JiT compilation error");
    expect(result.leftoverEnvironments).deep.equals([]);
  });

  test(
    "temporary environment is removed when a JiT action fails",
    { timeout: 120000 },
    async () => {
      const projectDir = await setupDataformCoreVersionProject(
        `publish("jit_table", { type: "table" }).jitCode(async (ctx) => {
          throw new Error("intentional JiT failure");
        });`,
      );

      const result = await runProject(projectDir, ["--dry-run", "--actions=jit_table"]);

      expect(result.exitCode).equals(1);
      expect(`${result.stdout}\n${result.stderr}`).to.include("intentional JiT failure");
      expect(result.leftoverEnvironments).deep.equals([]);
    },
  );

  test("package.json project uses the installed core version", { timeout: 120000 }, async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupProject(tmpDirFixture, projectDir);
    writeDefinitionFile(projectDir, "jit_table.js", CORE_VERSION_JIT_TABLE);

    expect(await dryRunJitTable(projectDir)).to.include(`SELECT '${version}' AS core_version`);
  });
});
