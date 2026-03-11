import { assert, expect } from "chai";
import { execFile } from "child_process";
import * as fs from "fs-extra";
import { load as loadYaml } from "js-yaml";
import * as path from "path";

import { cliEntryPointPath } from "df/cli/index_test_base";
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
import { getProcessResult, nodePath, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("init command", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

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

suite("init-creds command", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

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
