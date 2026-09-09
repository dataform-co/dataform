import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import {
  INTEGRATION_TEST_LOCATION,
  INTEGRATION_TEST_PROJECT,
  writeDefinitionFile
} from "df/cli/index_test_base";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("@dataform/integration/jit", { parallel: true }, ({ afterEach }) => {
  const credentials = dfapi.credentials.read("test_credentials/bigquery.json");
  const dbadapter = new BigQueryDbAdapter(credentials);
  const tmpDirFixture = new TmpDirFixture(afterEach);

  function setUpProject(projectDir: string, datasetName: string, tableName: string): void {
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: ${INTEGRATION_TEST_PROJECT}
defaultLocation: ${INTEGRATION_TEST_LOCATION}
defaultDataset: ${datasetName}
`
    );
    writeDefinitionFile(
      projectDir,
      "jit_table.js",
      `publish("${tableName}", { type: "table" }).jitCode(async (jctx) => "SELECT 1 as id")`
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
      JSON.stringify({ dependencies: { "@dataform/core": "3.0.0-alpha.0" } })
    );
  }

  test("JiT execution e2e", { timeout: 120000 }, async () => {
    // Create a simple project with a JiT table
    const projectDir = tmpDirFixture.createNewTmpDir();
    const datasetName = "df_integration_test_jit";
    const tableName = "jit_table";
    setUpProject(projectDir, datasetName, tableName);

    const compiledGraph = await dfapi.compile({ projectDir });

    // Drop dataset to start fresh
    await dbadapter.execute(
      `drop schema if exists \`${INTEGRATION_TEST_PROJECT}.${datasetName}\` cascade`
    );

    const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    const runResult = await dfapi.run(dbadapter, executionGraph, { projectDir }).result();

    expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
      dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
    );

    const rows = await dbadapter
      .execute(`SELECT * FROM \`${INTEGRATION_TEST_PROJECT}.${datasetName}.${tableName}\``)
      .then(res => res.rows);
    expect(rows).to.eql([{ id: 1 }]);
  });

  test("JiT dry run integration", { timeout: 120000 }, async () => {
    // Create a simple project with a JiT table
    const projectDir = tmpDirFixture.createNewTmpDir();
    const datasetName = "df_integration_test_jit_dry_run";
    const tableName = "jit_table_dry_run";
    setUpProject(projectDir, datasetName, tableName);

    const compiledGraph = await dfapi.compile({ projectDir });

    // Drop dataset to start fresh
    await dbadapter.execute(
      `drop schema if exists \`${INTEGRATION_TEST_PROJECT}.${datasetName}\` cascade`
    );

    const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);

    const runResult = await dfapi
      .run(dbadapter, executionGraph, {
        projectDir,
        bigquery: { dryRun: true }
      })
      .result();

    expect(dataform.RunResult.ExecutionStatus[runResult.status]).eql(
      dataform.RunResult.ExecutionStatus[dataform.RunResult.ExecutionStatus.SUCCESSFUL]
    );

    // Verify that the table was NOT created
    const tables = await dbadapter.schemas(INTEGRATION_TEST_PROJECT).then(schemas => {
      if (!schemas.includes(datasetName)) {
        return [];
      }
      return dbadapter.tables(INTEGRATION_TEST_PROJECT, datasetName);
    });
    expect(tables.length).to.equal(0);
  });
});
