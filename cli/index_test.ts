// tslint:disable tsr-detect-non-literal-fs-filename
import { assert, expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import { execFile } from "child_process";
import {
  ICEBERG_BUCKET_NAME_HINT,
  ICEBERG_BUCKET_NAME_PROMPT_QUESTION,
  ICEBERG_CONFIG_COLLECTED_TEXT,
  ICEBERG_CONFIG_PROMPT_HINT,
  ICEBERG_CONFIG_PROMPT_TEXT,
  ICEBERG_CONNECTION_HINT,
  ICEBERG_CONNECTION_QUESTION,
  ICEBERG_TABLE_FOLDER_ROOT_HINT,
  ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION,
  ICEBERG_TABLE_FOLDER_ROOT_SUBPATH_HINT,
  ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION
} from "df/cli/util";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { corePackageTarPath, getProcessResult, nodePath, npmPath, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

const DEFAULT_DATABASE = "dataform-open-source";
const DEFAULT_LOCATION = "US";
const CREDENTIALS_PATH = path.resolve(process.env.RUNFILES, "df/test_credentials/bigquery.json");

suite("@dataform/cli", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  const cliEntryPointPath = "cli/node_modules/@dataform/cli/bundle.js";

  suite("help command", () => {
    test("shows global help with the help command", async () => {
      const result = await getProcessResult(execFile(nodePath, [cliEntryPointPath, "help"]));
      expect(result.exitCode).equals(0);
      const output = result.stdout;
      expect(output).to.include("dataform [command]");
      expect(output).to.include("dataform init [project-dir] [default-database] [default-location]");
      expect(output).to.include("dataform install [project-dir]");
      expect(output).to.include("dataform init-creds [project-dir]");
      expect(output).to.include("dataform compile [project-dir]");
      expect(output).to.include("dataform test [project-dir]");
      expect(output).to.include("dataform run [project-dir]");
      expect(output).to.include("dataform format [project-dir]");
    });

    test("shows help for 'init' command", async () => {
      const result = await getProcessResult(execFile(nodePath, [cliEntryPointPath, "help", "init"]));
      expect(result.exitCode).equals(0);
      const output = result.stdout;
      expect(output).to.include("Create a new dataform project.");
      expect(output).to.include("--iceberg");
      expect(output).to.include("Initialize the project with workflow-level Iceberg tables configuration.");
    });
    test("shows help for 'install' command", async () => {
      const result = await getProcessResult(execFile(nodePath, [cliEntryPointPath, "help", "install"]));
      expect(result.exitCode).equals(0);
      const output = result.stdout;
      expect(output).to.include("Install a project's NPM dependencies.");
      expect(output).to.include("[project-dir]");
    });

    test("shows help for 'init-creds' command", async () => {
      const result = await getProcessResult(execFile(nodePath, [cliEntryPointPath, "help", "init-creds"]));
      expect(result.exitCode).equals(0);
      const output = result.stdout;
      expect(output).to.include("Create a .df-credentials.json file for Dataform to use when accessing BigQuery.");
      expect(output).to.include("[project-dir]");
      expect(output).to.include("--test-connection");
      expect(output).to.include("If true, a test query will be run using your final credentials.");
    });

    test("shows help for 'compile' command", async () => {
      const result = await getProcessResult(execFile(nodePath, [cliEntryPointPath, "help", "compile"]));
      expect(result.exitCode).equals(0);
      const output = result.stdout;
      expect(output).to.include("Compile the dataform project.");
      expect(output).to.include("--watch");
      expect(output).to.include("--json");
      expect(output).to.include("--quiet");
    });

    test("shows help for 'test' command", async () => {
      const result = await getProcessResult(execFile(nodePath, [cliEntryPointPath, "help", "test"]));
      expect(result.exitCode).equals(0);
      const output = result.stdout;
      expect(output).to.include("Run the dataform project's unit tests.");
      expect(output).to.include("[project-dir]");
      expect(output).to.include("--credentials");
      expect(output).to.include("--timeout");
      expect(output).to.include("--default-database");
      expect(output).to.include("--schema-suffix");
    });

    test("shows help for 'run' command", async () => {
      const result = await getProcessResult(execFile(nodePath, [cliEntryPointPath, "help", "run"]));
      expect(result.exitCode).equals(0);
      const output = result.stdout;
      expect(output).to.include("Run the dataform project.");
      expect(output).to.include("--dry-run");
      expect(output).to.include("--run-tests");
      expect(output).to.include("--action-retry-limit");
      expect(output).to.include("--actions");
      expect(output).to.include("--full-refresh");
      expect(output).to.include("--include-deps");
      expect(output).to.include("--include-dependents");
      expect(output).to.include("--tags");
      expect(output).to.include("--job-labels");
    });

     test("shows help for 'format' command", async () => {
      const result = await getProcessResult(execFile(nodePath, [cliEntryPointPath, "help", "format"]));
      expect(result.exitCode).equals(0);
      const output = result.stdout;
      expect(output).to.include("Format the dataform project's files.");
      expect(output).to.include("--check");
      expect(output).to.include("Check if files are formatted correctly without modifying them.");
      expect(output).to.include("--actions");

    });
  });

  suite("compile command", () => {
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
  "defaultLocation": "${DEFAULT_LOCATION}"
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
  });

  suite("init command", () => {
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

    suite("with --iceberg", () => {
      test("init with --iceberg sets defaultIcebergConfig with all fields", async () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        const testInputs = {
          [ICEBERG_BUCKET_NAME_PROMPT_QUESTION]: "my-iceberg-bucket",
          [ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION]: "my-iceberg-root",
          [ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION]: "my-iceberg-subpath",
          [ICEBERG_CONNECTION_QUESTION]: "my.default.connection",
        };

        const result = await getProcessResult(
          execFile(nodePath, [
            cliEntryPointPath,
            "init",
            projectDir,
            "dataform-iceberg-test",
            "us-central1",
            "--iceberg"
          ], {
            // Inject test inputs via environment variable
            env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) }
          })
        );

        expect(result.exitCode).equals(0);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_TEXT);
        expect(result.stdout).contains(ICEBERG_CONFIG_COLLECTED_TEXT);

        const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
        assert.isTrue(fs.existsSync(workflowSettingsPath));

        const workflowSettings = dataform.WorkflowSettings.create(
          loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
        );

        expect(workflowSettings.defaultIcebergConfig).to.deep.equal({
          bucketName: "my-iceberg-bucket",
          tableFolderRoot: "my-iceberg-root",
          tableFolderSubpath: "my-iceberg-subpath",
          connection: "my.default.connection",
        });
      });

      test("init with --iceberg handles empty inputs for bucketName", async () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        const testInputs = {
          [ICEBERG_BUCKET_NAME_PROMPT_QUESTION]: "", // Empty input
          [ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION]: "my-iceberg-root-with-empty-bucketName",
          [ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION]: "my-iceberg-subpath-with-empty-bucketName",
          [ICEBERG_CONNECTION_QUESTION]: "my.default.connection",
        };

        const result = await getProcessResult(
          execFile(nodePath, [
            cliEntryPointPath,
            "init",
            projectDir,
            "dataform-iceberg-partial",
            "us-east1",
            "--iceberg"
          ], {
            // Inject test inputs via environment variable
            env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) }
          })
        );

        expect(result.exitCode).equals(0);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_TEXT);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_HINT);
        expect(result.stdout).contains(ICEBERG_CONFIG_COLLECTED_TEXT);
        expect(result.stdout).contains(ICEBERG_BUCKET_NAME_HINT);
        expect(result.stdout).contains(ICEBERG_TABLE_FOLDER_ROOT_HINT);
        expect(result.stdout).contains(ICEBERG_TABLE_FOLDER_ROOT_SUBPATH_HINT);
        expect(result.stdout).contains(ICEBERG_CONNECTION_HINT);

        const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
        assert.isTrue(fs.existsSync(workflowSettingsPath));

        const workflowSettings = dataform.WorkflowSettings.create(
          loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
        );

        expect(workflowSettings.defaultIcebergConfig).to.deep.equal({
          tableFolderRoot: "my-iceberg-root-with-empty-bucketName",
          tableFolderSubpath: "my-iceberg-subpath-with-empty-bucketName",
          connection: "my.default.connection",
        });
      });

      test("init with --iceberg handles empty inputs for tableFolderRoot", async () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        const testInputs = {
          [ICEBERG_BUCKET_NAME_PROMPT_QUESTION]: "my-iceberg-bucket-with-empty-tablefolderroot",
          [ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION]: "", // Empty input
          [ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION]: "my-iceberg-subpath-with-empty-tableFolderRoot",
          [ICEBERG_CONNECTION_QUESTION]: "my.default.connection",
        };

        const result = await getProcessResult(
          execFile(nodePath, [
            cliEntryPointPath,
            "init",
            projectDir,
            "dataform-iceberg-partial",
            "us-east1",
            "--iceberg"
          ], {
            // Inject test inputs via environment variable
            env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) }
          })
        );

        expect(result.exitCode).equals(0);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_TEXT);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_HINT);
        expect(result.stdout).contains(ICEBERG_CONFIG_COLLECTED_TEXT);
        expect(result.stdout).contains(ICEBERG_BUCKET_NAME_HINT);
        expect(result.stdout).contains(ICEBERG_TABLE_FOLDER_ROOT_HINT);
        expect(result.stdout).contains(ICEBERG_TABLE_FOLDER_ROOT_SUBPATH_HINT);
        expect(result.stdout).contains(ICEBERG_CONNECTION_HINT);

        const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
        assert.isTrue(fs.existsSync(workflowSettingsPath));

        const workflowSettings = dataform.WorkflowSettings.create(
          loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
        );

        expect(workflowSettings.defaultIcebergConfig).to.deep.equal({
          bucketName: "my-iceberg-bucket-with-empty-tablefolderroot",
          tableFolderSubpath: "my-iceberg-subpath-with-empty-tableFolderRoot",
          connection: "my.default.connection",
        });
      });

      test("init with --iceberg handles empty inputs for tableFolderSubpath", async () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        const testInputs = {
          [ICEBERG_BUCKET_NAME_PROMPT_QUESTION]: "my-iceberg-bucket-with-empty-tablefoldersubpath",
          [ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION]: "my-iceberg-root-with-empty-tableFolderSubpath",
          [ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION]: "", // Empty input
          [ICEBERG_CONNECTION_QUESTION]: "my.default.connection",
        };

        const result = await getProcessResult(
          execFile(nodePath, [
            cliEntryPointPath,
            "init",
            projectDir,
            "dataform-iceberg-partial",
            "us-east1",
            "--iceberg"
          ], {
            // Inject test inputs via environment variable
            env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) }
          })
        );

        expect(result.exitCode).equals(0);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_TEXT);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_HINT);
        expect(result.stdout).contains(ICEBERG_CONFIG_COLLECTED_TEXT);
        expect(result.stdout).contains(ICEBERG_BUCKET_NAME_HINT);
        expect(result.stdout).contains(ICEBERG_TABLE_FOLDER_ROOT_HINT);
        expect(result.stdout).contains(ICEBERG_TABLE_FOLDER_ROOT_SUBPATH_HINT);
        expect(result.stdout).contains(ICEBERG_CONNECTION_HINT);

        const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
        assert.isTrue(fs.existsSync(workflowSettingsPath));

        const workflowSettings = dataform.WorkflowSettings.create(
          loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
        );

        expect(workflowSettings.defaultIcebergConfig).to.deep.equal({
          bucketName: "my-iceberg-bucket-with-empty-tablefoldersubpath",
          tableFolderRoot: "my-iceberg-root-with-empty-tableFolderSubpath",
          connection: "my.default.connection",
        });
      });

      test("init with --iceberg handles empty inputs for connection", async () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        const testInputs = {
          [ICEBERG_BUCKET_NAME_PROMPT_QUESTION]: "my-iceberg-bucket-with-empty-connection",
          [ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION]: "my-iceberg-root-with-empty-connection",
          [ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION]: "my-iceberg-subpath-with-empty-connection",
          [ICEBERG_CONNECTION_QUESTION]: "", // Empty input
        };

        const result = await getProcessResult(
          execFile(nodePath, [
            cliEntryPointPath,
            "init",
            projectDir,
            "dataform-iceberg-partial",
            "us-east1",
            "--iceberg"
          ], {
            // Inject test inputs via environment variable
            env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) }
          })
        );

        expect(result.exitCode).equals(0);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_TEXT);
        expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_HINT);
        expect(result.stdout).contains(ICEBERG_CONFIG_COLLECTED_TEXT);
        expect(result.stdout).contains(ICEBERG_BUCKET_NAME_HINT);
        expect(result.stdout).contains(ICEBERG_TABLE_FOLDER_ROOT_HINT);
        expect(result.stdout).contains(ICEBERG_TABLE_FOLDER_ROOT_SUBPATH_HINT);
        expect(result.stdout).contains(ICEBERG_CONNECTION_HINT);

        const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
        assert.isTrue(fs.existsSync(workflowSettingsPath));

        const workflowSettings = dataform.WorkflowSettings.create(
          loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
        );

        expect(workflowSettings.defaultIcebergConfig).to.deep.equal({
          bucketName: "my-iceberg-bucket-with-empty-connection",
          tableFolderRoot: "my-iceberg-root-with-empty-connection",
          tableFolderSubpath: "my-iceberg-subpath-with-empty-connection",
        });
      });
    });
  });

  suite("install command", () => {
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
        "No installation is needed when using workflow_settings.yaml, as packages are installed at " +
          "runtime."
      );
    });
  });

  suite("init-creds command", () => {
    test("init-creds fails for directory without dataform config", async () => {
      const emptyDir = tmpDirFixture.createNewTmpDir();
      const result = await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "init-creds", emptyDir])
      );
      expect(result.exitCode).to.not.equal(0);
      expect(result.stderr).to.include(
        `${emptyDir} does not appear to be a dataform directory (missing workflow_settings.yaml file).`
      );
    });
  });

  suite("format command", () => {
    test("test for format command", async () => {
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

      // Create a correctly formatted file
      const formattedFilePath = path.join(projectDir, "definitions", "formatted.sqlx");
      fs.ensureFileSync(formattedFilePath);
      fs.writeFileSync(
        formattedFilePath,
        `
config {
  type: "table"
}

SELECT
  1 AS test
`
      );

      // Create a file that needs formatting (extra spaces, inconsistent indentation)
      const unformattedFilePath = path.join(projectDir, "definitions", "unformatted.sqlx");
      fs.ensureFileSync(unformattedFilePath);
      fs.writeFileSync(
        unformattedFilePath,
        `
config {   type:  "table"   }
SELECT  1  as   test
`
      );

      // Test with --check flag on a project with files needing formatting
      const beforeFormatCheckResult = await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "format", projectDir, "--check"])
      );

      // Should exit with code 1 when files need formatting
      expect(beforeFormatCheckResult.exitCode).equals(1);
      expect(beforeFormatCheckResult.stderr).contains("Files that need formatting");
      expect(beforeFormatCheckResult.stderr).contains("unformatted.sqlx");

      // Format the files (without check flag)
      const formatCheckResult = await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "format", projectDir])
      );
      expect(formatCheckResult.exitCode).equals(0);

      // Test with --check flag after formatting
      const afterFormatCheckResult = await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "format", projectDir, "--check"])
      );

      // Should exit with code 0 when all files are properly formatted
      expect(afterFormatCheckResult.exitCode).equals(0);
      expect(afterFormatCheckResult.stdout).contains("All files are formatted correctly");
    });
  });

  suite("e2e tests", () => {
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
            type: "table"
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
  });

  suite("disable-assertions flag excludes assertions", ({ beforeEach }) => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    async function setupTestProject(): Promise<void> {
      const npmCacheDir = tmpDirFixture.createNewTmpDir();
      const packageJsonPath = path.join(projectDir, "package.json");

      await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "init", projectDir, DEFAULT_DATABASE, DEFAULT_LOCATION])
      );
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

    suite("from compilation", () => {
      const expectedCompileResult = {
        assertions: [
          {
            canonicalTarget: {
              database: DEFAULT_DATABASE,
              name: "dataform_example_table_assertions_uniqueKey_0",
              schema: "dataform_assertions"
            },
            dependencyTargets: [
              {
                database: DEFAULT_DATABASE,
                name: "example_table",
                schema: "dataform"
              }
            ],
            disabled: true,
            fileName: "definitions/example_table.sqlx",
            parentAction: {
              database: DEFAULT_DATABASE,
              name: "example_table",
              schema: "dataform"
            },
            query:
              // tslint:disable-next-line:tsr-detect-sql-literal-injection
              `\nSELECT\n  *\nFROM (\n  SELECT\n    id,\n    COUNT(1) AS index_row_count\n  FROM \`${DEFAULT_DATABASE}.dataform.example_table\`\n  GROUP BY id\n  ) AS data\nWHERE index_row_count > 1\n`,
            target: {
              database: DEFAULT_DATABASE,
              name: "dataform_example_table_assertions_uniqueKey_0",
              schema: "dataform_assertions"
            }
          },
          {
            canonicalTarget: {
              database: DEFAULT_DATABASE,
              name: "test_assertion",
              schema: "dataform_assertions"
            },
            disabled: true,
            fileName: "definitions/test_assertion.sqlx",
            query: "\n\nSELECT 1 WHERE FALSE\n",
            target: {
              database: DEFAULT_DATABASE,
              name: "test_assertion",
              schema: "dataform_assertions"
            }
          }
        ],
        dataformCoreVersion: version,
        graphErrors: {},
        jitData: {},
        projectConfig: {
          assertionSchema: "dataform_assertions",
          defaultDatabase: DEFAULT_DATABASE,
          defaultLocation: DEFAULT_LOCATION,
          defaultSchema: "dataform",
          disableAssertions: true,
          warehouse: "bigquery"
        },
        tables: [
          {
            canonicalTarget: {
              database: DEFAULT_DATABASE,
              name: "example_table",
              schema: "dataform"
            },
            disabled: false,
            enumType: "TABLE",
            fileName: "definitions/example_table.sqlx",
            hermeticity: "NON_HERMETIC",
            query: "\n\nSELECT 1 as id\n",
            target: {
              database: DEFAULT_DATABASE,
              name: "example_table",
              schema: "dataform"
            },
            type: "table"
          }
        ],
        targets: [
          {
            database: DEFAULT_DATABASE,
            name: "dataform_example_table_assertions_uniqueKey_0",
            schema: "dataform_assertions"
          },
          {
            database: DEFAULT_DATABASE,
            name: "example_table",
            schema: "dataform"
          },
          {
            database: DEFAULT_DATABASE,
            name: "test_assertion",
            schema: "dataform_assertions"
          }
        ]
      };

      test("with --disable-assertions flag", async () => {
        await setUpWorkflowSettings(false);

        const compileResult = await getProcessResult(
          execFile(nodePath, [
            cliEntryPointPath,
            "compile",
            projectDir,
            "--json",
            "--disable-assertions"
          ])
        );

        expect(compileResult.exitCode).equals(0);
        expect(JSON.parse(compileResult.stdout)).deep.equals(expectedCompileResult);
      });

      test("with disableAssertions set in workflow_settings.yaml", async () => {
        await setUpWorkflowSettings(true);

        const compileResult = await getProcessResult(
          execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--json"])
        );

        expect(compileResult.exitCode).equals(0);
        expect(JSON.parse(compileResult.stdout)).deep.equals(expectedCompileResult);
      });
    });

    suite("from run", () => {
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
            type: "table"
          },
          {
            fileName: "definitions/test_assertion.sqlx",
            hermeticity: "HERMETIC",
            target: {
              database: DEFAULT_DATABASE,
              name: "test_assertion",
              schema: "dataform_assertions"
            },
            type: "assertion"
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

        expect(runResult.exitCode).equals(0);
        expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
      });
    });
  });
  suite("--default-reservation flag", ({ beforeEach }) => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const RESERVATION = "projects/my-project/locations/us/reservations/my-reservation";

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
          `--default-reservation=${RESERVATION}`
        ])
      );

      expect(compileResult.exitCode).equals(0);
      const compiledGraph = JSON.parse(compileResult.stdout);
      expect(compiledGraph.projectConfig.defaultReservation).equals(RESERVATION);
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
          `--default-reservation=${RESERVATION}`,
          "--actions=example_table"
        ])
      );

      expect(runResult.exitCode).equals(0);
      const executionGraph = JSON.parse(runResult.stdout);
      expect(executionGraph.projectConfig.defaultReservation).equals(RESERVATION);
    });
  });
});
