import { expect } from "chai";
import { execFile, fork } from "child_process";
import { version } from "df/core/version";
import { suite, test } from "df/testing";
import * as fs from "fs";
import * as path from "path";

suite(__filename, () => {
  const nodePath = "external/nodejs_linux_amd64/bin/node";
  const npmPath = "external/nodejs_linux_amd64/bin/npm";
  const corePackageTarPath = "packages/@dataform/core/package.tgz";
  const cliEntryPointPath = "tests/cli/node_modules/@dataform/cli/bundle.js";

  const projectDir = path.join(process.env.TEST_TMPDIR, "project");
  const npmCacheDir = path.join(process.env.TEST_TMPDIR, "npm-cache");

  test("init and compile", async () => {
    // Initialize a project using the CLI, don't install packages.
    const initChildProcess = fork(cliEntryPointPath, [
      "init",
      "redshift",
      projectDir,
      "--skip-install"
    ]);

    await new Promise(resolve => {
      initChildProcess.on("close", resolve);
    });

    // Install packages manually to get around bazel sandbox issues.
    const installChildProcess = execFile(npmPath, [
      "install",
      "--prefix",
      projectDir,
      "--cache",
      npmCacheDir,
      corePackageTarPath
    ]);
    installChildProcess.stdout.pipe(process.stdout);
    installChildProcess.stderr.pipe(process.stderr);

    await new Promise(resolve => {
      installChildProcess.on("close", resolve);
    });

    // Write a simple file to the project.
    fs.writeFileSync(
      path.join(projectDir, "definitions", "example.sqlx"),
      `
config { type: "table" }
select 1 as test
`
    );

    // Compile the project using the CLI.
    const compileChildProcess = execFile(nodePath, [
      cliEntryPointPath,
      "compile",
      projectDir,
      "--json"
    ]);
    compileChildProcess.stdout.pipe(process.stdout);
    compileChildProcess.stderr.pipe(process.stderr);
    let chunks = "";
    compileChildProcess.stdout.on("data", chunk => (chunks += String(chunk)));

    expect(
      await new Promise(resolve => {
        compileChildProcess.on("close", resolve);
      })
    ).equals(0);

    expect(JSON.parse(chunks)).deep.equals({
      tables: [
        {
          name: "dataform.example",
          type: "table",
          target: {
            schema: "dataform",
            name: "example"
          },
          query: "\n\nselect 1 as test\n",
          disabled: false,
          fileName: "definitions/example.sqlx"
        }
      ],
      projectConfig: {
        warehouse: "redshift",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        useRunCache: false
      },
      graphErrors: {},
      dataformCoreVersion: version,
      targets: [
        {
          schema: "dataform",
          name: "example"
        }
      ]
    });
  });
});
