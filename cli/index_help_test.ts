import { expect } from "chai";
import { execFile } from "child_process";

import { cliEntryPointPath } from "df/cli/index_test_base";
import { getProcessResult, nodePath, suite, test } from "df/testing";

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
