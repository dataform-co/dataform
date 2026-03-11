import { expect } from "chai";
import { execFile } from "child_process";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import { cliEntryPointPath, DEFAULT_DATABASE, DEFAULT_LOCATION } from "df/cli/index_test_base";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { corePackageTarPath, getProcessResult, nodePath, npmPath, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("project ops", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("install command", () => {
    test("install throws an error when dataformCoreVersion in workflow_settings.yaml", async () => {
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
});
