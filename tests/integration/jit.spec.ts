import { expect } from "chai";
import * as fs from "fs-extra";
import * as os from "os";
import * as path from "path";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { INTEGRATION_TEST_LOCATION, INTEGRATION_TEST_PROJECT } from "df/cli/index_test_base";
import { dataform } from "df/protos/ts";
import { suite, test, writeDefinitionFile } from "df/testing";
import { dropDataset, makeSuffix } from "df/tests/integration/utils";

suite("@dataform/integration/jit", { parallel: true }, () => {
  const credentials = dfapi.credentials.read("test_credentials/bigquery.json");
  const dbadapter = new BigQueryDbAdapter(credentials);

  function setUpProject(projectDir: string, datasetName: string, tableName: string): void {
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: ${INTEGRATION_TEST_PROJECT}
defaultLocation: ${INTEGRATION_TEST_LOCATION}
defaultDataset: ${datasetName}
`,
    );
    writeDefinitionFile(
      projectDir,
      "jit_table.js",
      `publish("${tableName}", { type: "table" }).jitCode(async (jctx) => "SELECT 1 as id")`,
    );

    // Mock @dataform/core
    const nodeModulesDir = path.join(projectDir, "node_modules", "@dataform", "core");
    fs.mkdirpSync(nodeModulesDir);
    const coreBundlePath = path.resolve("packages/@dataform/core/bundle.js");
    fs.copyFileSync(coreBundlePath, path.join(nodeModulesDir, "bundle.js"));
    const corePackageJsonPath = path.resolve("packages/@dataform/core/package.json");
    fs.copyFileSync(corePackageJsonPath, path.join(nodeModulesDir, "package.json"));
    fs.writeFileSync(
      path.join(projectDir, "package.json"),
      JSON.stringify({ dependencies: { "@dataform/core": "3.0.0-alpha.0" } }),
    );
  }

  // Each test owns its project directory and its dataset, and removes both when it ends. The tests
  // run in parallel, so a cleanup shared by the suite (like TmpDirFixture with afterEach) would
  // delete the project of a test that's still running.
  async function withJitProject(
    name: string,
    fn: (projectDir: string, datasetName: string) => Promise<void>,
  ) {
    // TEST_TMPDIR is set by Bazel.
    const tmpDir = process.env.TEST_TMPDIR || os.tmpdir();
    const projectDir = fs.mkdtempSync(path.join(tmpDir, `${name}_`));
    const datasetName = `df_integration_test_${name}_${makeSuffix()}`;
    try {
      await fn(projectDir, datasetName);
    } finally {
      // Best effort: a cleanup error mustn't replace the error that failed the test, and one
      // cleanup step failing mustn't skip the other.
      try {
        fs.removeSync(projectDir);
      } catch (e) {
        process.stderr.write(`Could not remove ${projectDir}: ${e}\n`);
      }
      try {
        await dropDataset(dbadapter, datasetName);
      } catch (e) {
        process.stderr.write(`Could not drop dataset ${datasetName}: ${e}\n`);
      }
    }
  }

  function expectSuccessfulRun(runResult: dataform.IRunResult) {
    // Include the action results, so that a failure shows the underlying error message.
    expect(
      dataform.RunResult.ExecutionStatus[runResult.status],
      JSON.stringify(runResult.actions, null, 2),
    ).eql(dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]);
  }

  test("JiT execution e2e", { timeout: 120000 }, async () => {
    await withJitProject("jit", async (projectDir, datasetName) => {
      // Create a simple project with a JiT table
      const tableName = "jit_table";
      setUpProject(projectDir, datasetName, tableName);

      const compiledGraph = await dfapi.compile({ projectDir });
      const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
      const runResult = await dfapi.run(dbadapter, executionGraph, { projectDir }).result();

      expectSuccessfulRun(runResult);

      const rows = await dbadapter
        .execute(`SELECT * FROM \`${INTEGRATION_TEST_PROJECT}.${datasetName}.${tableName}\``)
        .then((res) => res.rows);
      expect(rows).to.eql([{ id: 1 }]);
    });
  });

  test("JiT dry run integration", { timeout: 120000 }, async () => {
    await withJitProject("jit_dry_run", async (projectDir, datasetName) => {
      // Create a simple project with a JiT table
      const tableName = "jit_table_dry_run";
      setUpProject(projectDir, datasetName, tableName);

      const compiledGraph = await dfapi.compile({ projectDir });
      const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);

      const runResult = await dfapi
        .run(dbadapter, executionGraph, {
          projectDir,
          bigquery: { dryRun: true },
        })
        .result();

      expectSuccessfulRun(runResult);

      // Verify that the table was NOT created
      const tables = await dbadapter.schemas(INTEGRATION_TEST_PROJECT).then((schemas) => {
        if (!schemas.includes(datasetName)) {
          return [];
        }
        return dbadapter.tables(INTEGRATION_TEST_PROJECT, datasetName);
      });
      expect(tables.length).to.equal(0);
    });
  });
});
