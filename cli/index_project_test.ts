import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { runCli, setupProject } from "df/cli/index_test_base";
import { suite, test, writeDefinitionFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("project ops", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("install command", () => {
    test("install throws an error when dataformCoreVersion in workflow_settings.yaml", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();

      await runCli("init", [
        projectDir,
        "--default-database=dataform-database",
        "--default-location=us-central1",
      ]);

      expect((await runCli("install", [projectDir])).stderr).contains(
        "No installation is needed when using workflow_settings.yaml, as packages are installed at " +
          "runtime.",
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
`,
      );

      // Create a file that needs formatting (extra spaces, inconsistent indentation)
      writeDefinitionFile(
        projectDir,
        "unformatted.sqlx",
        `
config {   type:  "table"   }
SELECT  1  as   test
`,
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
`,
      );

      const jsContents = `
function myCoolFn() {
  return true; }

modules.exports = {
  myCoolFn, }
`;
      const unformattedJsFilePath = path.join(projectDir, "includes", "someMod.js");
      fs.ensureFileSync(unformattedJsFilePath);
      fs.writeFileSync(unformattedJsFilePath, jsContents);

      // Run formatter
      const formatCmdRun = await runCli("format", [projectDir, "--ignore-js-files"]);

      expect(formatCmdRun.exitCode).equals(0);

      // Ensure the js file didn't change
      const bufFromFile = fs.readFileSync(unformattedJsFilePath);
      const bufFromContents = Buffer.from(jsContents, "utf-8");
      expect(bufFromContents.equals(bufFromFile)).equals(true);
    });

    test("format --check accepts correctly formatted files with CRLF line endings", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);
      writeDefinitionFile(
        projectDir,
        "crlf.sqlx",
        `
config {   type:  "table"   }
SELECT  1  as   test
`,
      );
      expect((await runCli("format", [projectDir])).exitCode).equals(0);

      // Same formatted content, but with Windows line endings (e.g. a Git checkout with
      // core.autocrlf=true).
      const crlfFilePath = path.join(projectDir, "definitions", "crlf.sqlx");
      const formattedContent = fs.readFileSync(crlfFilePath, "utf8");
      expect(formattedContent).not.contains("\r\n");
      fs.writeFileSync(crlfFilePath, formattedContent.replace(/\n/g, "\r\n"));

      const checkResult = await runCli("format", [projectDir, "--check"]);

      expect(checkResult.exitCode).equals(0);
      expect(checkResult.stdout).contains("All files are formatted correctly");
    });

    test("format rejects a symlink in definitions pointing outside projectDir", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const outsideDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      const externalSqlxPath = path.join(outsideDir, "external_table.sqlx");
      const unformattedExternalSqlx = 'config {   type:  "table" }\nselect   1';
      fs.writeFileSync(externalSqlxPath, unformattedExternalSqlx);
      fs.symlinkSync(externalSqlxPath, path.join(projectDir, "definitions", "linked.sqlx"));

      const formatResult = await runCli("format", [projectDir]);

      expect(formatResult.exitCode).equals(1);
      expect(formatResult.stdout).contains(
        'Refusing to format file outside project directory: "definitions/linked.sqlx"',
      );
      expect(fs.readFileSync(externalSqlxPath, "utf8")).equals(unformattedExternalSqlx);
    });

    test("format --check rejects a symlink in includes without reading its target", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const outsideDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      const externalJsPath = path.join(outsideDir, "external_helper.js");
      const invalidExternalJs = "DIAGNOSTIC_MARKER_TEXT {{{ unparseable";
      fs.writeFileSync(externalJsPath, invalidExternalJs);
      fs.ensureDirSync(path.join(projectDir, "includes"));
      fs.symlinkSync(externalJsPath, path.join(projectDir, "includes", "linked_helper.js"));

      const checkResult = await runCli("format", [projectDir, "--check"]);
      const combinedOutput = `${checkResult.stdout}\n${checkResult.stderr}`;

      expect(checkResult.exitCode).equals(1);
      expect(combinedOutput).contains(
        'Refusing to format file outside project directory: "includes/linked_helper.js"',
      );
      expect(combinedOutput).not.contains("DIAGNOSTIC_MARKER_TEXT");
      expect(fs.readFileSync(externalJsPath, "utf8")).equals(invalidExternalJs);
    });

    test("format rejects files reached through a directory symlink to outside projectDir", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const outsideDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      const externalSqlxPath = path.join(outsideDir, "external_table.sqlx");
      const unformattedExternalSqlx = 'config {   type:  "table" }\nselect   1';
      fs.writeFileSync(externalSqlxPath, unformattedExternalSqlx);
      fs.symlinkSync(outsideDir, path.join(projectDir, "definitions", "ext"));

      const formatResult = await runCli("format", [
        projectDir,
        "--actions",
        "definitions/ext/*.sqlx",
      ]);

      expect(formatResult.exitCode).equals(1);
      expect(formatResult.stdout).contains(
        'Refusing to format file outside project directory: "definitions/ext/external_table.sqlx"',
      );
      expect(fs.readFileSync(externalSqlxPath, "utf8")).equals(unformattedExternalSqlx);
    });

    test("format rejects --actions patterns that leave projectDir", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const outsideDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      const externalJsPath = path.join(outsideDir, "outside.js");
      const unformattedJs = "const   x=1;const y=   2;";
      fs.writeFileSync(externalJsPath, unformattedJs);
      const relativeEscapeGlob = path
        .join(path.relative(projectDir, outsideDir), "*.js")
        .replace(/\\/g, "/");

      const traversalResult = await runCli("format", [projectDir, "--actions", relativeEscapeGlob]);

      expect(traversalResult.exitCode).equals(1);
      expect(traversalResult.stdout).contains("Refusing to format file outside project directory");
      expect(fs.readFileSync(externalJsPath, "utf8")).equals(unformattedJs);
    });

    test("format fails on a dangling symlink without creating its target", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const outsideDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      const missingExternalPath = path.join(outsideDir, "nonexistent.sqlx");
      fs.symlinkSync(missingExternalPath, path.join(projectDir, "definitions", "dangling.sqlx"));

      const danglingResult = await runCli("format", [projectDir]);

      expect(danglingResult.exitCode).equals(1);
      expect(danglingResult.stdout).contains("definitions/dangling.sqlx: ENOENT");
      expect(fs.existsSync(missingExternalPath)).equals(false);
    });

    test("format works when projectDir is itself a symlink to the project", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const linkParentDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);
      writeDefinitionFile(projectDir, "table.sqlx", 'config {   type:  "table" }\nSELECT  1\n');
      const linkedProjectDir = path.join(linkParentDir, "project_link");
      fs.symlinkSync(projectDir, linkedProjectDir, "dir");

      const formatResult = await runCli("format", [linkedProjectDir]);

      expect(formatResult.exitCode).equals(0);
      expect(fs.readFileSync(path.join(projectDir, "definitions", "table.sqlx"), "utf8")).equals(
        'config {\n  type: "table"\n}\n\nSELECT\n  1\n',
      );
    });

    test("formats intra-project symlinks and dot-prefixed filenames inside projectDir", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      const sharedSqlxPath = path.join(projectDir, "includes", "shared_query.sqlx");
      fs.ensureFileSync(sharedSqlxPath);
      fs.writeFileSync(sharedSqlxPath, 'config {   type:  "view" }\nSELECT  1\n');
      fs.symlinkSync(
        path.join("..", "includes", "shared_query.sqlx"),
        path.join(projectDir, "definitions", "alias_query.sqlx"),
      );

      const dotPrefixedPath = path.join(projectDir, "definitions", "..dotprefixed.sqlx");
      fs.writeFileSync(dotPrefixedPath, 'config {   type:  "table" }\nSELECT  2\n');

      const formatResult = await runCli("format", [
        projectDir,
        "--actions",
        "definitions/alias_query.sqlx,definitions/..dotprefixed.sqlx",
      ]);

      expect(formatResult.exitCode).equals(0);
      expect(fs.readFileSync(sharedSqlxPath, "utf8")).equals(
        'config {\n  type: "view"\n}\n\nSELECT\n  1\n',
      );
      expect(fs.readFileSync(dotPrefixedPath, "utf8")).equals(
        'config {\n  type: "table"\n}\n\nSELECT\n  2\n',
      );
    });
  });

  suite("run command", () => {
    test("--override-protected without --full-refresh is rejected", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();

      await runCli("init", [
        projectDir,
        "--default-database=dataform-database",
        "--default-location=us-central1",
      ]);

      const result = await runCli("run", [projectDir, "--override-protected"]);

      expect(result.exitCode).not.equals(0);
      expect(result.stderr).contains(
        "The --override-protected flag can only be supplied along with --full-refresh.",
      );
    });
  });
});
