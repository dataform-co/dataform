import { expect } from "chai";
import { spawn } from "child_process";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import { TEMPORARY_ENVIRONMENT_PREFIX } from "df/cli/api/commands/compile";
import {
  alterWorkflowSettings,
  cliEntryPointPath,
  INTEGRATION_TEST_LOCATION,
  INTEGRATION_TEST_PROJECT,
  runCli,
  setupProject,
  writeWorkflowSettingsWithCoreVersion,
} from "df/cli/index_test_base";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { getProcessResult, nodePath, suite, test, writeDefinitionFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("compile", () => {
  suite("compile command", ({ afterEach }) => {
    const tmpDirFixture = new TmpDirFixture(afterEach);

    test(
      "compile throws an error when dataformCoreVersion not in workflow_settings.yaml and no " +
        "package.json exists",
      async () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          dumpYaml(dataform.WorkflowSettings.create({ defaultProject: "dataform" })),
        );

        expect((await runCli("compile", [projectDir])).stderr).contains(
          "dataformCoreVersion must be specified either in workflow_settings.yaml or via a " +
            "package.json",
        );
      },
    );

    test("compile error when package.json and no package is installed", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "package.json"),
        `{
  "dependencies":{
    "@dataform/core": "${version}"
  }
}`,
      );
      fs.writeFileSync(
        path.join(projectDir, "dataform.json"),
        `{
  "defaultDatabase": "tada-analytics",
  "defaultSchema": "df_integration_test",
  "assertionSchema": "df_integration_test_assertions",
  "defaultLocation": "${INTEGRATION_TEST_LOCATION}"
}
`,
      );

      const stderr = (await runCli("compile", [projectDir])).stderr;
      expect(stderr).contains("Could not find @dataform/core:");
      expect(stderr).contains(
        "Run `dataform install` in the project directory to install the version from package.json.",
      );
    });

    test(
      "compile rejects @dataform/core with incompatible version",
      { timeout: 60000 },
      async () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        // dataformCoreVersion in workflow_settings.yaml triggers the stateless
        // install path (compile.ts copies to a tmp dir and runs `npm i`), so the
        // test exercises the same flow real users hit. 2.9.0 is the latest 2.x on
        // the registry; its major (2) is incompatible with the current CLI (3.x).
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          dumpYaml(
            dataform.WorkflowSettings.create({
              defaultProject: "dataform",
              dataformCoreVersion: "2.9.0",
            }),
          ),
        );

        // npm needs a writable cache; ~/.npm is read-only in the bazel sandbox.
        const npmCacheDir = tmpDirFixture.createNewTmpDir();
        const stderr = (
          await runCli("compile", [projectDir], {
            env: { ...process.env, NPM_CONFIG_CACHE: npmCacheDir },
          })
        ).stderr;
        expect(stderr).contains("@dataform/core 2.9.0 is not compatible with @dataform/cli");
        expect(stderr).contains("The CLI requires @dataform/core >=3.0.0 <4.0.0");
        expect(stderr).contains("Set `dataformCoreVersion:");
      },
    );

    // Core versions older than 3.0.57 can't tell which project file is running when the CLI
    // runs them in its VM, so the CLI patches them (the caller-file shim). These tests check
    // that the shim still gives actions the right fileName. 3.0.0 is MIN_SUPPORTED_CORE_VERSION
    // (cli/vm/compile.ts), so it must keep working when the CLI version goes up.
    ["3.0.0", "3.0.50"].forEach((coreVersion) => {
      test(
        `compile succeeds with @dataform/core ${coreVersion} via caller-file shim`,
        { timeout: 60000 },
        async () => {
          const projectDir = tmpDirFixture.createNewTmpDir();
          fs.writeFileSync(
            path.join(projectDir, "workflow_settings.yaml"),
            dumpYaml({
              defaultProject: INTEGRATION_TEST_PROJECT,
              defaultLocation: INTEGRATION_TEST_LOCATION,
              defaultDataset: "dataform",
              dataformCoreVersion: coreVersion,
            }),
          );
          writeDefinitionFile(
            projectDir,
            "example.sqlx",
            `config { type: "table" }\nSELECT 1 AS id`,
          );

          const npmCacheDir = tmpDirFixture.createNewTmpDir();
          const result = await runCli("compile", [projectDir, "--json"], {
            env: { ...process.env, NPM_CONFIG_CACHE: npmCacheDir },
          });

          expect(result.exitCode, `compile failed: ${result.stderr}`).equals(0);
          const compiled = JSON.parse(result.stdout);
          expect(compiled.tables).to.have.lengthOf(1);
          expect(compiled.tables[0].fileName).equals("definitions/example.sqlx");
        },
      );
    });

    ["package.json", "package-lock.json", "node_modules"].forEach((npmFile) => {
      test(`compile throws an error when dataformCoreVersion in workflow_settings.yaml and ${npmFile} is present`, async () => {
        const projectDir = tmpDirFixture.createNewTmpDir();
        fs.writeFileSync(
          path.join(projectDir, "workflow_settings.yaml"),
          dumpYaml(
            dataform.WorkflowSettings.create({
              defaultProject: "dataform",
              dataformCoreVersion: "3.0.0",
            }),
          ),
        );
        const resolvedNpmPath = path.join(projectDir, npmFile);
        if (npmFile === "node_modules") {
          fs.mkdirSync(resolvedNpmPath);
        } else {
          fs.writeFileSync(resolvedNpmPath, "");
        }

        expect((await runCli("compile", [projectDir])).stderr).contains(
          `${npmFile}' unexpected; remove it and try again`,
        );
      });
    });

    // Runs the CLI with a fake `npm` that records its pid, marks that it has started and then
    // hangs; once the installation has started, sends `signal` either to the CLI's whole process
    // group (like Ctrl+C in a terminal) or to the CLI process only (like `kill <pid>`), and reports
    // how the CLI exited. The fake `npm` makes the timing deterministic and needs no network.
    async function interruptDuringCoreInstallation(
      command: string,
      signal: "SIGINT" | "SIGTERM",
      target: "processGroup" | "cliProcess",
    ) {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsWithCoreVersion(projectDir, version);
      const tmpDir = tmpDirFixture.createNewTmpDir();
      const fakeBinDir = tmpDirFixture.createNewTmpDir();
      const npmPidFile = path.join(tmpDirFixture.createNewTmpDir(), "npm.pid");
      fs.writeFileSync(
        path.join(fakeBinDir, "npm"),
        `#!/bin/sh\necho $$ > "${npmPidFile}"\ntouch npm-started\nexec sleep 600\n`,
        { mode: 0o755 },
      );

      // A separate process group, like a job in a terminal.
      const cli = spawn(nodePath, [cliEntryPointPath, command, projectDir], {
        detached: true,
        env: {
          ...process.env,
          TMPDIR: tmpDir,
          PATH: `${fakeBinDir}${path.delimiter}${process.env.PATH}`,
        },
      });
      const result = getProcessResult(cli);
      const temporaryEnvironments = () =>
        fs.readdirSync(tmpDir).filter((entry) => entry.startsWith(TEMPORARY_ENVIRONMENT_PREFIX));
      try {
        await waitUntil(() =>
          temporaryEnvironments().some((entry) =>
            fs.existsSync(path.join(tmpDir, entry, "npm-started")),
          ),
        );
        process.kill(target === "processGroup" ? -cli.pid : cli.pid, signal);
      } catch (e) {
        process.kill(-cli.pid, "SIGKILL");
        throw e;
      }
      const { exitCode } = await result;

      const npmPid = Number(fs.readFileSync(npmPidFile, "utf8"));
      const npmStopped = await waitUntil(() => !isRunning(npmPid), 5000).then(
        () => true,
        () => false,
      );
      if (!npmStopped) {
        // Don't leave the fake npm behind when the test fails.
        process.kill(npmPid, "SIGKILL");
      }
      return { exitCode, leftoverEnvironments: temporaryEnvironments(), npmStopped };
    }

    // The fake npm, process groups and process.kill(-pid) are POSIX-only.
    if (process.platform !== "win32") {
      ["compile", "run"].forEach((command) => {
        test(
          `Ctrl+C during the @dataform/core installation of ${command} removes the temporary ` +
            "environment",
          { timeout: 60000 },
          async () => {
            const result = await interruptDuringCoreInstallation(command, "SIGINT", "processGroup");

            expect(result.exitCode).equals(130);
            expect(result.leftoverEnvironments).deep.equals([]);
            expect(result.npmStopped).equals(true);
          },
        );
      });

      test(
        "SIGTERM sent only to the CLI process stops npm and removes the temporary environment",
        { timeout: 60000 },
        async () => {
          const result = await interruptDuringCoreInstallation("compile", "SIGTERM", "cliProcess");

          expect(result.exitCode).equals(143);
          expect(result.npmStopped).equals(true);
          expect(result.leftoverEnvironments).deep.equals([]);
        },
      );
    }
  });

  suite("disable-assertions flag (compilation)", ({ afterEach, beforeEach }) => {
    const tmpDirFixture = new TmpDirFixture(afterEach);
    let projectDir: string;

    async function setupTestProject(): Promise<void> {
      await setupProject(tmpDirFixture, projectDir);

      writeDefinitionFile(
        projectDir,
        "test_assertion.sqlx",
        `
config { type: "assertion" }
SELECT 1 WHERE FALSE
`,
      );

      writeDefinitionFile(
        projectDir,
        "example_table.sqlx",
        `
config {
  type: "table",
  assertions: {
    uniqueKey: ["id"]
  }
}
SELECT 1 as id
`,
      );
    }

    beforeEach("setup test project", async () => {
      projectDir = tmpDirFixture.createNewTmpDir();
      await setupTestProject();
    });

    const expectedCompileResult = {
      assertions: [
        {
          canonicalTarget: {
            database: INTEGRATION_TEST_PROJECT,
            name: "dataform_example_table_assertions_uniqueKey_0",
            schema: "dataform_assertions",
          },
          dependencyTargets: [
            {
              database: INTEGRATION_TEST_PROJECT,
              name: "example_table",
              schema: "dataform",
            },
          ],
          disabled: true,
          fileName: "definitions/example_table.sqlx",
          parentAction: {
            database: INTEGRATION_TEST_PROJECT,
            name: "example_table",
            schema: "dataform",
          },
          query: `\nSELECT\n  *\nFROM (\n  SELECT\n    id,\n    COUNT(1) AS index_row_count\n  FROM \`${INTEGRATION_TEST_PROJECT}.dataform.example_table\`\n  GROUP BY id\n  ) AS data\nWHERE index_row_count > 1\n`,
          target: {
            database: INTEGRATION_TEST_PROJECT,
            name: "dataform_example_table_assertions_uniqueKey_0",
            schema: "dataform_assertions",
          },
        },
        {
          canonicalTarget: {
            database: INTEGRATION_TEST_PROJECT,
            name: "test_assertion",
            schema: "dataform_assertions",
          },
          disabled: true,
          fileName: "definitions/test_assertion.sqlx",
          query: "\n\nSELECT 1 WHERE FALSE\n",
          target: {
            database: INTEGRATION_TEST_PROJECT,
            name: "test_assertion",
            schema: "dataform_assertions",
          },
        },
      ],
      dataformCoreVersion: version,
      graphErrors: {},
      jitData: {},
      projectConfig: {
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        defaultSchema: "dataform",
        disableAssertions: true,
        warehouse: "bigquery",
      },
      tables: [
        {
          canonicalTarget: {
            database: INTEGRATION_TEST_PROJECT,
            name: "example_table",
            schema: "dataform",
          },
          disabled: false,
          enumType: "TABLE",
          fileName: "definitions/example_table.sqlx",
          hermeticity: "NON_HERMETIC",
          query: "\n\nSELECT 1 as id\n",
          target: {
            database: INTEGRATION_TEST_PROJECT,
            name: "example_table",
            schema: "dataform",
          },
          type: "table",
        },
      ],
      targets: [
        {
          database: INTEGRATION_TEST_PROJECT,
          name: "dataform_example_table_assertions_uniqueKey_0",
          schema: "dataform_assertions",
        },
        {
          database: INTEGRATION_TEST_PROJECT,
          name: "example_table",
          schema: "dataform",
        },
        {
          database: INTEGRATION_TEST_PROJECT,
          name: "test_assertion",
          schema: "dataform_assertions",
        },
      ],
    };

    test("with --disable-assertions flag", async () => {
      alterWorkflowSettings(projectDir, { disableAssertions: false });

      const compileResult = await runCli("compile", [projectDir, "--json", "--disable-assertions"]);

      expect(compileResult.exitCode).equals(0);
      expect(JSON.parse(compileResult.stdout)).deep.equals(expectedCompileResult);
    });

    test("with disableAssertions set in workflow_settings.yaml", async () => {
      alterWorkflowSettings(projectDir, { disableAssertions: true });

      const compileResult = await runCli("compile", [projectDir, "--json"]);

      expect(compileResult.exitCode).equals(0);
      expect(JSON.parse(compileResult.stdout)).deep.equals(expectedCompileResult);
    });
  });

  suite("compile node selection", ({ afterEach, beforeEach }) => {
    const tmpDirFixture = new TmpDirFixture(afterEach);

    const tableNames = (stdout: string): string[] =>
      JSON.parse(stdout)
        .tables.map((table: any) => table.target.name)
        .sort();

    let projectDir: string;

    beforeEach("setup test project", async () => {
      // Builds a project with three tables: upstream -> midstream -> downstream.
      projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      writeDefinitionFile(
        projectDir,
        "upstream.sqlx",
        `config { type: "table", tags: ["daily"] }\nSELECT 1 AS id`,
      );
      writeDefinitionFile(
        projectDir,
        "midstream.sqlx",
        `config { type: "table" }\nSELECT * FROM \${ref("upstream")}`,
      );
      writeDefinitionFile(
        projectDir,
        "downstream.sqlx",
        `config { type: "table" }\nSELECT * FROM \${ref("midstream")}`,
      );
    });

    test("no selector emits the entire graph", async () => {
      const result = await runCli("compile", [projectDir, "--json"]);
      expect(result.exitCode, result.stderr).equals(0);
      expect(tableNames(result.stdout)).deep.equals(["downstream", "midstream", "upstream"]);
    });

    test("--output-actions filters output to the selected action", async () => {
      const result = await runCli("compile", [
        projectDir,
        "--output-actions",
        "midstream",
        "--json",
      ]);
      expect(result.exitCode, result.stderr).equals(0);
      expect(tableNames(result.stdout)).deep.equals(["midstream"]);
      // `targets` is printed alongside the actions, so it must be pruned too.
      expect(JSON.parse(result.stdout).targets.map((target: any) => target.name)).deep.equals([
        "midstream",
      ]);
    });

    test("--output-actions --output-include-deps pulls in upstream dependencies", async () => {
      const result = await runCli("compile", [
        projectDir,
        "--output-actions",
        "midstream",
        "--output-include-deps",
        "--json",
      ]);
      expect(result.exitCode, result.stderr).equals(0);
      expect(tableNames(result.stdout)).deep.equals(["midstream", "upstream"]);
    });

    test("--output-actions --output-include-dependents pulls in downstream dependents", async () => {
      const result = await runCli("compile", [
        projectDir,
        "--output-actions",
        "midstream",
        "--output-include-dependents",
        "--json",
      ]);
      expect(result.exitCode, result.stderr).equals(0);
      expect(tableNames(result.stdout)).deep.equals(["downstream", "midstream"]);
    });

    test("--output-tags filters output to actions carrying the tag", async () => {
      const result = await runCli("compile", [projectDir, "--output-tags", "daily", "--json"]);
      expect(result.exitCode, result.stderr).equals(0);
      expect(tableNames(result.stdout)).deep.equals(["upstream"]);
    });

    test("selector matching nothing emits an empty graph and exits zero", async () => {
      const result = await runCli("compile", [projectDir, "--output-actions", "nope", "--json"]);
      expect(result.exitCode, result.stderr).equals(0);
      expect(tableNames(result.stdout)).deep.equals([]);
    });

    test("--output-include-deps without a selector is rejected", async () => {
      const result = await runCli("compile", [projectDir, "--output-include-deps", "--json"]);
      expect(result.exitCode).not.equals(0);
      expect(result.stderr).contains("--output-include-deps");
    });
  });

  suite("extension config", ({ afterEach }) => {
    const tmpDirFixture = new TmpDirFixture(afterEach);

    test("compile succeeds with extension set in workflow_settings.yaml", async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir, {
        defaultDataset: "dataform",
        defaultAssertionDataset: "dataform_assertions",
        extension: {
          name: "test-extension",
          compilationMode: dataform.ExtensionCompilationMode.PROLOGUE,
        },
      });

      const compileResult = await runCli("compile", [projectDir]);

      expect(compileResult.exitCode).equals(0);
      expect(compileResult.stdout).contains("Compiled 0 action(s).");
    });
  });
});

function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return e.code === "EPERM";
  }
}

async function waitUntil(condition: () => boolean, timeoutMillis = 30000) {
  const deadline = Date.now() + timeoutMillis;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error(`Condition not met within ${timeoutMillis}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}
