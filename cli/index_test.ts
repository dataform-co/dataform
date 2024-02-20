// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as os from "os";
import * as path from "path";

import { ChildProcess, execFile } from "child_process";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("@dataform/cli", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  const platformPath = os.platform() === "darwin" ? "nodejs_darwin_amd64" : "nodejs_linux_amd64";
  const nodePath = `external/${platformPath}/bin/node`;
  const cliEntryPointPath = "cli/node_modules/@dataform/cli/bundle.js";
  const npmPath = `external/${platformPath}/bin/npm`;
  const corePackageTarPath = "packages/@dataform/core/package.tgz";

  test(
    "compile throws an error when dataformCoreVersion not in workflow_settings.yaml and no " +
      "package.json exists",
    async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        dumpYaml(dataform.WorkflowSettings.create({ defaultProject: "dataform" }))
      );

      expect(
        (await getProcessResult(execFile(nodePath, [cliEntryPointPath, "compile", projectDir])))
          .stderr
      ).contains(
        "dataformCoreVersion must be specified either in workflow_settings.yaml or via a " +
          "package.json"
      );
    }
  );

  test("compile error when package.json and no package is installed", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "package.json"),
      `{
  "dependencies":{
    "@dataform/core": "${version}"
  }
}`
    );
    fs.writeFileSync(
      path.join(projectDir, "dataform.json"),
      `{
  "defaultDatabase": "tada-analytics",
  "defaultSchema": "df_integration_test",
  "assertionSchema": "df_integration_test_assertions",
  "defaultLocation": "US"
}
`
    );

    expect(
      (await getProcessResult(execFile(nodePath, [cliEntryPointPath, "compile", projectDir])))
        .stderr
    ).contains(
      "Could not find a recent installed version of @dataform/core in the project. Check that " +
        "either `dataformCoreVersion` is specified in `workflow_settings.yaml`, or " +
        "`@dataform/core` is specified in `package.json`. If using `package.json`, then run " +
        "`dataform install`."
    );
  });

  test("workflow_settings.yaml generated from init", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "init",
        projectDir,
        "--default-database=dataform-database",
        "--default-location=us-central1"
      ])
    );

    expect(fs.readFileSync(path.join(projectDir, "workflow_settings.yaml"), "utf8")).to
      .equal(`dataformCoreVersion: ${version}
defaultProject: dataform-database
defaultLocation: us-central1
defaultDataset: dataform
defaultAssertionDataset: dataform_assertions
`);
  });

  test("install throws an error when dataformCoreVersion in workflow_settings.yaml", async () => {
    // When dataformCoreVersion is managed by workflow_settings.yaml, installation is stateless and
    // lazy; it happens when compile is called, or otherwise as needed.

    const projectDir = tmpDirFixture.createNewTmpDir();

    await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "init",
        projectDir,
        "--default-database=dataform-database",
        "--default-location=us-central1"
      ])
    );

    expect(
      (await getProcessResult(execFile(nodePath, [cliEntryPointPath, "install", projectDir])))
        .stderr
    ).contains(
      "Package installation is only supported when specifying @dataform/core version in " +
        "'package.json'"
    );
  });

  ["package.json", "package-lock.json", "node_modules"].forEach(npmFile => {
    test(`compile throws an error when dataformCoreVersion in workflow_settings.yaml and ${npmFile} is present`, async () => {
      // When dataformCoreVersion is managed by workflow_settings.yaml, installation is stateless and
      // lazy; it happens when compile is called, or otherwise as needed.
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        dumpYaml(
          dataform.WorkflowSettings.create({
            defaultProject: "dataform",
            dataformCoreVersion: "3.0.0"
          })
        )
      );
      const resolvedNpmPath = path.join(projectDir, npmFile);
      if (npmFile === "node_modules") {
        fs.mkdirSync(resolvedNpmPath);
      } else {
        fs.writeFileSync(resolvedNpmPath, "");
      }

      expect(
        (await getProcessResult(execFile(nodePath, [cliEntryPointPath, "compile", projectDir])))
          .stderr
      ).contains(`${npmFile}' unexpected; remove it and try again`);
    });
  });

  test("golden path with package.json", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const npmCacheDir = tmpDirFixture.createNewTmpDir();
    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const packageJsonPath = path.join(projectDir, "package.json");

    // Initialize a project using the CLI, don't install packages.
    await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "init",
        projectDir,
        "dataform-integration-tests",
        "US"
      ])
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
config { type: "table" }
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
            database: "dataform-integration-tests",
            schema: "dataform_test_schema_suffix",
            name: "example"
          },
          canonicalTarget: {
            schema: "dataform",
            name: "example",
            database: "dataform-integration-tests"
          },
          query: "\n\nselect 1 as testValue2\n",
          disabled: false,
          fileName: "definitions/example.sqlx"
        }
      ],
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: "dataform-integration-tests",
        defaultLocation: "US",
        vars: {
          testVar1: "testValue1",
          testVar2: "testValue2"
        },
        schemaSuffix: "test_schema_suffix"
      },
      graphErrors: {},
      dataformCoreVersion: version,
      targets: [
        {
          database: "dataform-integration-tests",
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
        "test_credentials/bigquery.json",
        "--dry-run",
        "--json",
        "--vars=testVar1=testValue1,testVar2=testValue2",
        "--default-location=europe"
      ])
    );

    expect(runResult.exitCode).equals(0);

    expect(JSON.parse(runResult.stdout)).deep.equals({
      actions: [
        {
          fileName: "definitions/example.sqlx",
          hermeticity: "HERMETIC",
          tableType: "table",
          target: {
            database: "dataform-integration-tests",
            name: "example",
            schema: "dataform"
          },
          tasks: [
            {
              statement:
                "create or replace table `dataform-integration-tests.dataform.example` as \n\nselect 1 as testValue2",
              type: "statement"
            }
          ],
          type: "table"
        }
      ],
      projectConfig: {
        assertionSchema: "dataform_assertions",
        defaultDatabase: "dataform-integration-tests",
        defaultLocation: "europe",
        defaultSchema: "dataform",
        warehouse: "bigquery",
        vars: {
          testVar1: "testValue1",
          testVar2: "testValue2"
        }
      },
      runConfig: {
        fullRefresh: false
      },
      warehouseState: {}
    });
  });
});

async function getProcessResult(childProcess: ChildProcess) {
  let stderr = "";
  childProcess.stderr.pipe(process.stderr);
  childProcess.stderr.on("data", chunk => (stderr += String(chunk)));
  let stdout = "";
  childProcess.stdout.pipe(process.stdout);
  childProcess.stdout.on("data", chunk => (stdout += String(chunk)));
  const exitCode: number = await new Promise(resolve => {
    childProcess.on("close", resolve);
  });
  return { exitCode, stdout, stderr };
}
