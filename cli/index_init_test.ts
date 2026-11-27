import { assert, expect } from "chai";
import * as fs from "fs-extra";
import { load as loadYaml } from "js-yaml";
import * as path from "path";

import { runCli } from "df/cli/index_test_base";
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
  ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION,
  LAKEHOUSE_CATALOG_HINT,
  LAKEHOUSE_CATALOG_PROMPT_QUESTION,
  LAKEHOUSE_CONFIG_COLLECTED_TEXT,
  LAKEHOUSE_CONFIG_PROMPT_HINT,
  LAKEHOUSE_CONFIG_PROMPT_TEXT,
  LAKEHOUSE_NAMESPACE_HINT,
  LAKEHOUSE_NAMESPACE_PROMPT_QUESTION,
} from "df/cli/util";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("init command", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("workflow_settings.yaml generated from init", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    await runCli("init", [
      projectDir,
      "--default-database=dataform-database",
      "--default-location=us-central1",
    ]);

    expect(fs.readFileSync(path.join(projectDir, "workflow_settings.yaml"), "utf8")).to
      .equal(`dataformCoreVersion: ${version}
defaultProject: dataform-database
defaultLocation: us-central1
defaultDataset: dataform
defaultAssertionDataset: dataform_assertions
`);
  });

  test("workflow_settings.yaml generated from init without a location", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    const result = await runCli("init", [projectDir, "--default-database=dataform-database"]);

    expect(result.exitCode).equals(0);
    expect(fs.readFileSync(path.join(projectDir, "workflow_settings.yaml"), "utf8")).to
      .equal(`dataformCoreVersion: ${version}
defaultProject: dataform-database
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

      const result = await runCli(
        "init",
        [projectDir, "dataform-iceberg-test", "us-central1", "--iceberg"],
        {
          // Inject test inputs via environment variable
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
      );

      expect(result.exitCode).equals(0);
      expect(result.stdout).contains(ICEBERG_CONFIG_PROMPT_TEXT);
      expect(result.stdout).contains(ICEBERG_CONFIG_COLLECTED_TEXT);

      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      assert.isTrue(fs.existsSync(workflowSettingsPath));

      const workflowSettings = dataform.WorkflowSettings.create(
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")),
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

      const result = await runCli(
        "init",
        [projectDir, "dataform-iceberg-partial", "us-east1", "--iceberg"],
        {
          // Inject test inputs via environment variable
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
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
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")),
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
        [ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION]:
          "my-iceberg-subpath-with-empty-tableFolderRoot",
        [ICEBERG_CONNECTION_QUESTION]: "my.default.connection",
      };

      const result = await runCli(
        "init",
        [projectDir, "dataform-iceberg-partial", "us-east1", "--iceberg"],
        {
          // Inject test inputs via environment variable
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
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
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")),
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
        [ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION]:
          "my-iceberg-root-with-empty-tableFolderSubpath",
        [ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION]: "", // Empty input
        [ICEBERG_CONNECTION_QUESTION]: "my.default.connection",
      };

      const result = await runCli(
        "init",
        [projectDir, "dataform-iceberg-partial", "us-east1", "--iceberg"],
        {
          // Inject test inputs via environment variable
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
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
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")),
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

      const result = await runCli(
        "init",
        [projectDir, "dataform-iceberg-partial", "us-east1", "--iceberg"],
        {
          // Inject test inputs via environment variable
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
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
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")),
      );

      expect(workflowSettings.defaultIcebergConfig).to.deep.equal({
        bucketName: "my-iceberg-bucket-with-empty-connection",
        tableFolderRoot: "my-iceberg-root-with-empty-connection",
        tableFolderSubpath: "my-iceberg-subpath-with-empty-connection",
      });
    });
  });

  suite("with --lakehouse", () => {
    test("init without --lakehouse does not write defaultLakehouseConfig", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();

      const result = await runCli("init", [projectDir, "dataform-no-lakehouse", "us-central1"]);

      expect(result.exitCode).equals(0);
      expect(result.stdout).not.contains(LAKEHOUSE_CONFIG_PROMPT_TEXT);

      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const rawYaml = fs.readFileSync(workflowSettingsPath, "utf8");
      expect(rawYaml).not.contains("defaultLakehouseConfig");

      const parsedYaml = loadYaml(rawYaml) as dataform.IWorkflowSettings;
      expect(parsedYaml.defaultLakehouseConfig).equals(undefined);
    });

    test("init with --lakehouse sets defaultLakehouseConfig with catalog and namespace", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const testInputs = {
        [LAKEHOUSE_CATALOG_PROMPT_QUESTION]: "my_catalog",
        [LAKEHOUSE_NAMESPACE_PROMPT_QUESTION]: "my_namespace",
      };

      const result = await runCli(
        "init",
        [projectDir, "dataform-lakehouse-test", "us-central1", "--lakehouse"],
        {
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
      );

      expect(result.exitCode).equals(0);
      expect(result.stdout).contains(LAKEHOUSE_CONFIG_PROMPT_TEXT);
      expect(result.stdout).contains(LAKEHOUSE_CONFIG_PROMPT_HINT);
      expect(result.stdout).contains(LAKEHOUSE_CATALOG_HINT);
      expect(result.stdout).contains(LAKEHOUSE_NAMESPACE_HINT);
      expect(result.stdout).contains(LAKEHOUSE_CONFIG_COLLECTED_TEXT);

      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      assert.isTrue(fs.existsSync(workflowSettingsPath));

      const rawYaml = fs.readFileSync(workflowSettingsPath, "utf8");
      expect(rawYaml).not.contains("defaultIcebergConfig");

      const workflowSettings = dataform.WorkflowSettings.create(loadYaml(rawYaml));
      expect(workflowSettings.defaultLakehouseConfig).to.deep.equal({
        catalog: "my_catalog",
        namespace: "my_namespace",
      });
    });

    test("init with --lakehouse rejects empty, whitespace, and dotted catalog inputs before accepting valid input", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const testInputs = {
        [LAKEHOUSE_CATALOG_PROMPT_QUESTION]: ["", "   ", "bad.catalog", "  valid_catalog  "],
        [LAKEHOUSE_NAMESPACE_PROMPT_QUESTION]: "valid_namespace",
      };

      const result = await runCli(
        "init",
        [projectDir, "dataform-lakehouse-retry-cat", "us-central1", "--lakehouse"],
        {
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
      );

      expect(result.exitCode).equals(0);
      expect(result.stderr).contains("Validation Error: Lakehouse catalog cannot be empty.");
      expect(result.stderr).contains("Validation Error: Lakehouse catalog cannot contain '.'.");
      expect(result.stdout).contains("Please try again.");

      const workflowSettings = dataform.WorkflowSettings.create(
        loadYaml(fs.readFileSync(path.join(projectDir, "workflow_settings.yaml"), "utf8")),
      );
      expect(workflowSettings.defaultLakehouseConfig).to.deep.equal({
        catalog: "valid_catalog",
        namespace: "valid_namespace",
      });
    });

    test("init with --lakehouse rejects empty, whitespace, and dotted namespace inputs before accepting valid input", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const testInputs = {
        [LAKEHOUSE_CATALOG_PROMPT_QUESTION]: "valid_catalog",
        [LAKEHOUSE_NAMESPACE_PROMPT_QUESTION]: ["", "   ", "bad.namespace", "  valid_namespace  "],
      };

      const result = await runCli(
        "init",
        [projectDir, "dataform-lakehouse-retry-ns", "us-central1", "--lakehouse"],
        {
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
      );

      expect(result.exitCode).equals(0);
      expect(result.stderr).contains("Validation Error: Lakehouse namespace cannot be empty.");
      expect(result.stderr).contains("Validation Error: Lakehouse namespace cannot contain '.'.");
      expect(result.stdout).contains("Please try again.");

      const workflowSettings = dataform.WorkflowSettings.create(
        loadYaml(fs.readFileSync(path.join(projectDir, "workflow_settings.yaml"), "utf8")),
      );
      expect(workflowSettings.defaultLakehouseConfig).to.deep.equal({
        catalog: "valid_catalog",
        namespace: "valid_namespace",
      });
    });

    test("init with --lakehouse fails without writing workflow_settings.yaml when only invalid input is provided", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const testInputs = {
        [LAKEHOUSE_CATALOG_PROMPT_QUESTION]: "invalid.catalog",
        [LAKEHOUSE_NAMESPACE_PROMPT_QUESTION]: "valid_namespace",
      };

      const result = await runCli(
        "init",
        [projectDir, "dataform-lakehouse-fail", "us-central1", "--lakehouse"],
        {
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
      );

      expect(result.exitCode).to.not.equal(0);
      expect(result.stderr).contains("Validation Error: Lakehouse catalog cannot contain '.'.");
      assert.isFalse(fs.existsSync(path.join(projectDir, "workflow_settings.yaml")));
    });

    test("init with --iceberg and --lakehouse writes both defaultIcebergConfig and defaultLakehouseConfig", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const testInputs = {
        [ICEBERG_BUCKET_NAME_PROMPT_QUESTION]: "my-iceberg-bucket",
        [ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION]: "my-iceberg-root",
        [ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION]: "my-iceberg-subpath",
        [ICEBERG_CONNECTION_QUESTION]: "my.default.connection",
        [LAKEHOUSE_CATALOG_PROMPT_QUESTION]: "my-lakehouse-catalog",
        [LAKEHOUSE_NAMESPACE_PROMPT_QUESTION]: "my_lakehouse_namespace",
      };

      const result = await runCli(
        "init",
        [projectDir, "dataform-both-test", "us-central1", "--iceberg", "--lakehouse"],
        {
          env: { ...process.env, DATAFORM_CLI_TEST_INPUTS: JSON.stringify(testInputs) },
        },
      );

      expect(result.exitCode).equals(0);
      expect(result.stdout).contains(ICEBERG_CONFIG_COLLECTED_TEXT);
      expect(result.stdout).contains(LAKEHOUSE_CONFIG_COLLECTED_TEXT);

      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      assert.isTrue(fs.existsSync(workflowSettingsPath));

      const workflowSettings = dataform.WorkflowSettings.create(
        loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")),
      );
      expect(workflowSettings.defaultIcebergConfig).to.deep.equal({
        bucketName: "my-iceberg-bucket",
        tableFolderRoot: "my-iceberg-root",
        tableFolderSubpath: "my-iceberg-subpath",
        connection: "my.default.connection",
      });
      expect(workflowSettings.defaultLakehouseConfig).to.deep.equal({
        catalog: "my-lakehouse-catalog",
        namespace: "my_lakehouse_namespace",
      });
    });
  });
});

suite("init-creds command", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("init-creds fails for directory without dataform config", async () => {
    const emptyDir = tmpDirFixture.createNewTmpDir();
    const result = await runCli("init-creds", [emptyDir]);
    expect(result.exitCode).to.not.equal(0);
    expect(result.stderr).to.include(
      `${emptyDir} does not appear to be a dataform directory (missing workflow_settings.yaml file).`,
    );
  });
});
