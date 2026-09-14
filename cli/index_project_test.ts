import { expect } from "chai";
import * as fs from "fs-extra";
import { createServer } from "http";
import { AddressInfo } from "net";
import * as path from "path";

import {
  runCli,
  setupProject
} from "df/cli/index_test_base";
import { suite, test, writeDefinitionFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("project ops", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("project setup installs the local core package without registry requests", async () => {
    const requests: string[] = [];
    const registry = createServer((request, response) => {
      requests.push(request.url);
      response.writeHead(503);
      response.end();
    });
    await new Promise<void>(resolve => registry.listen(0, "127.0.0.1", resolve));
    const previousRegistry = process.env.npm_config_registry;
    const previousRetries = process.env.npm_config_fetch_retries;
    try {
      process.env.npm_config_registry = `http://127.0.0.1:${(registry.address() as AddressInfo).port}`;
      process.env.npm_config_fetch_retries = "0";
      const projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      expect(fs.existsSync(path.join(projectDir, "node_modules/@dataform/core/bundle.js"))).equals(true);
      expect(requests).deep.equals([]);
    } finally {
      if (previousRegistry === undefined) {
        delete process.env.npm_config_registry;
      } else {
        process.env.npm_config_registry = previousRegistry;
      }
      if (previousRetries === undefined) {
        delete process.env.npm_config_fetch_retries;
      } else {
        process.env.npm_config_fetch_retries = previousRetries;
      }
      await new Promise<void>(resolve => registry.close(() => resolve()));
    }
  });

  suite("install command", () => {
    test("install throws an error when dataformCoreVersion in workflow_settings.yaml", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();

      await runCli("init", [
        projectDir,
        "--default-database=dataform-database",
        "--default-location=us-central1"
      ]);

      expect(
        (await runCli("install", [projectDir])).stderr
      ).contains(
        "No installation is needed when using workflow_settings.yaml, as packages are installed at " +
          "runtime."
      );
    });
  });

  suite("format command", () => {
    test("test for format command", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      // Create a correctly formatted file
      writeDefinitionFile(
        projectDir,
        "formatted.sqlx",
        `
config {
  type: "table"
}

SELECT
  1 AS test
`
      );

      // Create a file that needs formatting (extra spaces, inconsistent indentation)
      writeDefinitionFile(
        projectDir,
        "unformatted.sqlx",
        `
config {   type:  "table"   }
SELECT  1  as   test
`
      );

      // Test with --check flag on a project with files needing formatting
      const beforeFormatCheckResult = await runCli("format", [projectDir, "--check"]);

      // Should exit with code 1 when files need formatting
      expect(beforeFormatCheckResult.exitCode).equals(1);
      expect(beforeFormatCheckResult.stderr).contains("Files that need formatting");
      expect(beforeFormatCheckResult.stderr).contains("unformatted.sqlx");

      // Format the files (without check flag)
      const formatCheckResult = await runCli("format", [projectDir]);
      expect(formatCheckResult.exitCode).equals(0);

      // Test with --check flag after formatting
      const afterFormatCheckResult = await runCli("format", [projectDir, "--check"]);

      // Should exit with code 0 when all files are properly formatted
      expect(afterFormatCheckResult.exitCode).equals(0);
      expect(afterFormatCheckResult.stdout).contains("All files are formatted correctly");
    });

    test("test for format command ignore js files", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      // Create files that need formatting and ensure that the js file is not modified
      writeDefinitionFile(
        projectDir,
        "unformatted.sqlx",
        `
config {   type:  "table"   }
SELECT  1  as   test
`
      );

      const jsContents = `
function myCoolFn() {
  return true; }

modules.exports = {
  myCoolFn, }
`;
      const unformattedJsFilePath = path.join(projectDir, "includes", "someMod.js");
      fs.ensureFileSync(unformattedJsFilePath);
      fs.writeFileSync(
        unformattedJsFilePath,
        jsContents,
      );

      // Run formatter
      const formatCmdRun = await runCli("format", [projectDir, "--ignore-js-files"]);

      expect(formatCmdRun.exitCode).equals(0);

      // Ensure the js file didn't change
      const bufFromFile = fs.readFileSync(unformattedJsFilePath);
      const bufFromContents = Buffer.from(jsContents, "utf-8");
      expect(bufFromContents.equals(bufFromFile)).equals(true);
    });
  });
});
