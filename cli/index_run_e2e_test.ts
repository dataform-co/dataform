import { expect } from "chai";
import { execFile } from "child_process";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import {
  cliEntryPointPath,
  CREDENTIALS_PATH,
  DEFAULT_DATABASE,
  DEFAULT_LOCATION,
  DEFAULT_RESERVATION
} from "df/cli/index_test_base";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { corePackageTarPath, getProcessResult, nodePath, npmPath, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("run e2e", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("golden path with package.json", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const npmCacheDir = tmpDirFixture.createNewTmpDir();
    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const packageJsonPath = path.join(projectDir, "package.json");

    // Initialize a project using the CLI, don't install packages.
    await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "init", projectDir, DEFAULT_DATABASE, DEFAULT_LOCATION])
    );

    // Install packages manually to get around bazel read-only sandbox issues.
    const workflowSettings = dataform.WorkflowSettings.create(
      loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
    );
    delete workflowSettings.dataformCoreVersion;
    fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));
    fs.writeFileSync(
      packageJsonPath,
      `{
  "dependencies":{
    "@dataform/core": "${version}"
  }
}`
    );
    await getProcessResult(
      execFile(npmPath, [
        "install",
        "--prefix",
        projectDir,
        "--cache",
        npmCacheDir,
        corePackageTarPath
      ])
    );

    // Write a simple file to the project.
    const filePath = path.join(projectDir, "definitions", "example.sqlx");
    fs.ensureFileSync(filePath);
    fs.writeFileSync(
      filePath,
      `
config { type: "table", tags: ["someTag"] }
select 1 as \${dataform.projectConfig.vars.testVar2}
`
    );

    // Compile the project using the CLI.
    const compileResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "compile",
        projectDir,
        "--json",
        "--vars=testVar1=testValue1,testVar2=testValue2",
        "--schema-suffix=test_schema_suffix"
      ])
    );

    expect(compileResult.exitCode).equals(0);

    expect(JSON.parse(compileResult.stdout)).deep.equals({
      tables: [
        {
          type: "table",
          enumType: "TABLE",
          target: {
            database: DEFAULT_DATABASE,
            schema: "dataform_test_schema_suffix",
            name: "example"
          },
          canonicalTarget: {
            schema: "dataform",
            name: "example",
            database: DEFAULT_DATABASE
          },
          query: "\n\nselect 1 as testValue2\n",
          disabled: false,
          fileName: "definitions/example.sqlx",
          hermeticity: "NON_HERMETIC",
          tags: ["someTag"]
        }
      ],
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: DEFAULT_DATABASE,
        defaultLocation: DEFAULT_LOCATION,
        vars: {
          testVar1: "testValue1",
          testVar2: "testValue2"
        },
        schemaSuffix: "test_schema_suffix"
      },
      graphErrors: {},
      jitData: {},
      dataformCoreVersion: version,
      targets: [
        {
          database: DEFAULT_DATABASE,
          schema: "dataform",
          name: "example"
        }
      ]
    });

    // Dry run the project.
    const runResult = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "run",
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--vars=testVar1=testValue1,testVar2=testValue2",
        "--default-location=europe",
        "--tags=someTag,someOtherTag",
        "--actions=example,someOtherAction"
      ])
    );

    if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
      // tslint:disable-next-line:no-console
      console.error("GOLDEN PATH FAILED. STDERR:", runResult.stderr);
    }
    expect(runResult.exitCode).equals(0);

    expect(JSON.parse(runResult.stdout)).deep.equals({
      actions: [
        {
          fileName: "definitions/example.sqlx",
          hermeticity: "NON_HERMETIC",
          tableType: "table",
          target: {
            database: DEFAULT_DATABASE,
            name: "example",
            schema: "dataform"
          },
          tasks: [
            {
              statement:
                // tslint:disable-next-line:tsr-detect-sql-literal-injection
                `create or replace table \`${DEFAULT_DATABASE}.dataform.example\` as \n\nselect 1 as testValue2`,
              type: "statement"
            }
          ],
          type: "table",
        }
      ],
      projectConfig: {
        assertionSchema: "dataform_assertions",
        defaultDatabase: DEFAULT_DATABASE,
        defaultLocation: "europe",
        defaultSchema: "dataform",
        warehouse: "bigquery",
        vars: {
          testVar1: "testValue1",
          testVar2: "testValue2"
        }
      },
      runConfig: {
        fullRefresh: false,
        tags: ["someTag", "someOtherTag"],
        actions: ["example", "someOtherAction"]
      },
      warehouseState: {}
    });
  });

  suite("disable-assertions flag (run)", ({ beforeEach }) => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    async function setupTestProject(): Promise<void> {
      const npmCacheDir = tmpDirFixture.createNewTmpDir();
      const packageJsonPath = path.join(projectDir, "package.json");

      await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "init", projectDir, DEFAULT_DATABASE, DEFAULT_LOCATION])
      );

      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const workflowSettings = dataform.WorkflowSettings.create(
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
      );
      delete workflowSettings.dataformCoreVersion;
      fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));

      fs.writeFileSync(
        packageJsonPath,
        `{
  "dependencies":{
    "@dataform/core": "${version}"
  }
}`
      );
      await getProcessResult(
        execFile(npmPath, [
          "install",
          "--prefix",
          projectDir,
          "--cache",
          npmCacheDir,
          corePackageTarPath
        ])
      );

      const assertionFilePath = path.join(projectDir, "definitions", "test_assertion.sqlx");
      fs.ensureFileSync(assertionFilePath);
      fs.writeFileSync(
        assertionFilePath,
        `
config { type: "assertion" }
SELECT 1 WHERE FALSE
`
      );

      const tableFilePath = path.join(projectDir, "definitions", "example_table.sqlx");
      fs.ensureFileSync(tableFilePath);
      fs.writeFileSync(
        tableFilePath,
        `
config {
  type: "table",
  assertions: {
    uniqueKey: ["id"]
  }
}
SELECT 1 as id
`
      );
    }

    async function setUpWorkflowSettings(disableAssertions: boolean): Promise<void> {
      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const workflowSettings = dataform.WorkflowSettings.create(
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
      );
      delete workflowSettings.dataformCoreVersion;
      workflowSettings.disableAssertions = disableAssertions;
      fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));
    }

    beforeEach("setup test project", async () => await setupTestProject());

    const expectedRunResult = {
      actions: [
        {
          fileName: "definitions/example_table.sqlx",
          hermeticity: "NON_HERMETIC",
          tableType: "table",
          target: {
            database: DEFAULT_DATABASE,
            name: "example_table",
            schema: "dataform"
          },
          tasks: [
            {
              statement:
                // tslint:disable-next-line:tsr-detect-sql-literal-injection
                `create or replace table \`${DEFAULT_DATABASE}.dataform.example_table\` as \n\nSELECT 1 as id`,
              type: "statement"
            }
          ],
          type: "table",
        },
        {
          fileName: "definitions/test_assertion.sqlx",
          hermeticity: "HERMETIC",
          target: {
            database: DEFAULT_DATABASE,
            name: "test_assertion",
            schema: "dataform_assertions"
          },
          type: "assertion",
        }
      ],
      projectConfig: {
        assertionSchema: "dataform_assertions",
        defaultDatabase: DEFAULT_DATABASE,
        defaultLocation: DEFAULT_LOCATION,
        defaultSchema: "dataform",
        disableAssertions: true,
        warehouse: "bigquery"
      },
      runConfig: {
        actions: ["test_assertion", "example_table"],
        fullRefresh: false
      },
      warehouseState: {}
    };

    test("with --disable-assertions flag", async () => {
      await setUpWorkflowSettings(false);

      const runResult = await getProcessResult(
        execFile(nodePath, [
          cliEntryPointPath,
          "run",
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          "--disable-assertions",
          "--actions=test_assertion,example_table"
        ])
      );

      if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
        // tslint:disable-next-line:no-console
        console.error("ASSERTIONS TEST FAILED. STDERR:", runResult.stderr);
      }
      expect(runResult.exitCode).equals(0);
      expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
    });

    test("with disableAssertions set in workflow_settings.yaml", async () => {
      await setUpWorkflowSettings(true);

      const runResult = await getProcessResult(
        execFile(nodePath, [
          cliEntryPointPath,
          "run",
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          "--actions=test_assertion,example_table"
        ])
      );

      if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
        // tslint:disable-next-line:no-console
        console.error("ASSERTIONS TEST FAILED. STDERR:", runResult.stderr);
      }
      expect(runResult.exitCode).equals(0);
      expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
    });

    test("with --job-labels flag", async () => {
      await setUpWorkflowSettings(false);

      const runResult = await getProcessResult(
        execFile(nodePath, [
          cliEntryPointPath,
          "run",
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          "--disable-assertions",
          "--actions=test_assertion,example_table",
          "--job-labels=env=testing,team=dataform"
        ])
      );

      if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
        // tslint:disable-next-line:no-console
        console.error("ASSERTIONS TEST FAILED. STDERR:", runResult.stderr);
      }
      expect(runResult.exitCode).equals(0);
      expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
    });
  });


  suite("--default-reservation flag", ({ beforeEach }) => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    beforeEach("setup test project", async () => {
      const npmCacheDir = tmpDirFixture.createNewTmpDir();
      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const packageJsonPath = path.join(projectDir, "package.json");

      await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "init", projectDir, DEFAULT_DATABASE, DEFAULT_LOCATION])
      );

      // Remove dataformCoreVersion so we can use the local package.
      const workflowSettings = dataform.WorkflowSettings.create(
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
      );
      delete workflowSettings.dataformCoreVersion;
      fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));

      fs.writeFileSync(
        packageJsonPath,
        `{
  "dependencies":{
    "@dataform/core": "${version}"
  }
}`
      );
      await getProcessResult(
        execFile(npmPath, [
          "install",
          "--prefix",
          projectDir,
          "--cache",
          npmCacheDir,
          corePackageTarPath
        ])
      );

      const tableFilePath = path.join(projectDir, "definitions", "example_table.sqlx");
      fs.ensureFileSync(tableFilePath);
      fs.writeFileSync(
        tableFilePath,
        `
config { type: "table" }
SELECT 1 as id
`
      );
    });

    test("--default-reservation flag is applied to projectConfig in compile output", async () => {
      const compileResult = await getProcessResult(
        execFile(nodePath, [
          cliEntryPointPath,
          "compile",
          projectDir,
          "--json",
          `--default-reservation=${DEFAULT_RESERVATION}`
        ])
      );

      expect(compileResult.exitCode).equals(0);
      const compiledGraph = JSON.parse(compileResult.stdout);
      expect(compiledGraph.projectConfig).deep.equals({
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: DEFAULT_DATABASE,
        defaultLocation: DEFAULT_LOCATION,
        defaultReservation: DEFAULT_RESERVATION
      });
    });

    test("--default-reservation flag is applied to projectConfig in run (dry-run) output", async () => {
      const runResult = await getProcessResult(
        execFile(nodePath, [
          cliEntryPointPath,
          "run",
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          `--default-reservation=${DEFAULT_RESERVATION}`,
          "--actions=example_table"
        ])
      );

      expect(runResult.exitCode).equals(0);
      const executionGraph = JSON.parse(runResult.stdout);
      expect(executionGraph.projectConfig).deep.equals({
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: DEFAULT_DATABASE,
        defaultLocation: DEFAULT_LOCATION,
        defaultReservation: DEFAULT_RESERVATION
      });
    });
  });
});
