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

suite("compile command", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

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

  test("compile rejects @dataform/core with incompatible version", async () => {
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
          dataformCoreVersion: "2.9.0"
        })
      )
    );

    // npm needs a writable cache; ~/.npm is read-only in the bazel sandbox.
    const npmCacheDir = tmpDirFixture.createNewTmpDir();
    const stderr = (
      await getProcessResult(
        execFile(nodePath, [cliEntryPointPath, "compile", projectDir], {
          env: { ...process.env, NPM_CONFIG_CACHE: npmCacheDir }
        })
      )
    ).stderr;
    expect(stderr).contains("@dataform/core 2.9.0 is not compatible with @dataform/cli");
    expect(stderr).contains("matching major.minor");
    expect(stderr).contains("Set `dataformCoreVersion:");
  });

  test("compile succeeds with @dataform/core <= 3.0.56 via caller-file shim", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    // 3.0.50 predates 3.0.57, which is when @dataform/core started reading
    // global.__dataform_current_file as a fallback in getCallerFile(). The
    // compile path text-patches the bundle to add that fallback; this test
    // proves the patch + host-side file stack drive a real action's
    // fileName from inside vm2 3.11.3's path-stripped sandbox.
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      dumpYaml({
        defaultProject: DEFAULT_DATABASE,
        defaultLocation: DEFAULT_LOCATION,
        defaultDataset: "dataform",
        dataformCoreVersion: "3.0.50"
      })
    );
    fs.ensureFileSync(path.join(projectDir, "definitions", "example.sqlx"));
    fs.writeFileSync(
      path.join(projectDir, "definitions", "example.sqlx"),
      `config { type: "table" }\nSELECT 1 AS id`
    );

    const npmCacheDir = tmpDirFixture.createNewTmpDir();
    const result = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--json"], {
        env: { ...process.env, NPM_CONFIG_CACHE: npmCacheDir }
      })
    );

    expect(result.exitCode, `compile failed: ${result.stderr}`).equals(0);
    const compiled = JSON.parse(result.stdout);
    expect(compiled.tables).to.have.lengthOf(1);
    expect(compiled.tables[0].fileName).equals("definitions/example.sqlx");
  });

  ["package.json", "package-lock.json", "node_modules"].forEach(npmFile => {
    test(`compile throws an error when dataformCoreVersion in workflow_settings.yaml and ${npmFile} is present`, async () => {
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

suite("disable-assertions flag (compilation)", ({ afterEach, beforeEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  let projectDir: string;

  async function setupTestProject(): Promise<void> {
    const npmCacheDir = tmpDirFixture.createNewTmpDir();
    const packageJsonPath = path.join(projectDir, "package.json");

    await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "init", projectDir, DEFAULT_DATABASE, DEFAULT_LOCATION])
    );

    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
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
    workflowSettings.disableAssertions = disableAssertions;
    fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));
  }

  beforeEach("setup test project", async () => {
    projectDir = tmpDirFixture.createNewTmpDir();
    await setupTestProject();
  });

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

suite("compile node selection", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  // Builds a project with three tables: upstream -> midstream -> downstream.
  async function setupSelectionProject(): Promise<string> {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const npmCacheDir = tmpDirFixture.createNewTmpDir();

    await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "init", projectDir, DEFAULT_DATABASE, DEFAULT_LOCATION])
    );

    const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
    const workflowSettings = dataform.WorkflowSettings.create(
      loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
    );
    delete workflowSettings.dataformCoreVersion;
    fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));

    fs.writeFileSync(
      path.join(projectDir, "package.json"),
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

    const def = (name: string, contents: string) => {
      const filePath = path.join(projectDir, "definitions", `${name}.sqlx`);
      fs.ensureFileSync(filePath);
      fs.writeFileSync(filePath, contents);
    };
    def("upstream", `config { type: "table", tags: ["daily"] }\nSELECT 1 AS id`);
    def("midstream", `config { type: "table" }\nSELECT * FROM \${ref("upstream")}`);
    def("downstream", `config { type: "table" }\nSELECT * FROM \${ref("midstream")}`);

    return projectDir;
  }

  const tableNames = (stdout: string): string[] =>
    JSON.parse(stdout).tables.map((table: any) => table.target.name).sort();

  test("no selector emits the entire graph", async () => {
    const projectDir = await setupSelectionProject();
    const result = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--json"])
    );
    expect(result.exitCode, result.stderr).equals(0);
    expect(tableNames(result.stdout)).deep.equals(["downstream", "midstream", "upstream"]);
  });

  test("--output-actions filters output to the selected action", async () => {
    const projectDir = await setupSelectionProject();
    const result = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--output-actions", "midstream", "--json"])
    );
    expect(result.exitCode, result.stderr).equals(0);
    expect(tableNames(result.stdout)).deep.equals(["midstream"]);
  });

  test("--output-actions --output-include-deps pulls in upstream dependencies", async () => {
    const projectDir = await setupSelectionProject();
    const result = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "compile",
        projectDir,
        "--output-actions",
        "midstream",
        "--output-include-deps",
        "--json"
      ])
    );
    expect(result.exitCode, result.stderr).equals(0);
    expect(tableNames(result.stdout)).deep.equals(["midstream", "upstream"]);
  });

  test("--output-actions --output-include-dependents pulls in downstream dependents", async () => {
    const projectDir = await setupSelectionProject();
    const result = await getProcessResult(
      execFile(nodePath, [
        cliEntryPointPath,
        "compile",
        projectDir,
        "--output-actions",
        "midstream",
        "--output-include-dependents",
        "--json"
      ])
    );
    expect(result.exitCode, result.stderr).equals(0);
    expect(tableNames(result.stdout)).deep.equals(["downstream", "midstream"]);
  });

  test("--output-tags filters output to actions carrying the tag", async () => {
    const projectDir = await setupSelectionProject();
    const result = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--output-tags", "daily", "--json"])
    );
    expect(result.exitCode, result.stderr).equals(0);
    expect(tableNames(result.stdout)).deep.equals(["upstream"]);
  });

  test("selector matching nothing emits an empty graph and exits zero", async () => {
    const projectDir = await setupSelectionProject();
    const result = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--output-actions", "nope", "--json"])
    );
    expect(result.exitCode, result.stderr).equals(0);
    expect(tableNames(result.stdout)).deep.equals([]);
  });

  test("--output-include-deps without a selector is rejected", async () => {
    const projectDir = await setupSelectionProject();
    const result = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir, "--output-include-deps", "--json"])
    );
    expect(result.exitCode).not.equals(0);
    expect(result.stderr).contains("--output-include-deps");
  });
});

suite("extension config", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("compile succeeds with extension set in workflow_settings.yaml", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const npmCacheDir = tmpDirFixture.createNewTmpDir();

    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      dumpYaml({
        defaultProject: DEFAULT_DATABASE,
        defaultLocation: DEFAULT_LOCATION,
        defaultDataset: "dataform",
        defaultAssertionDataset: "dataform_assertions",
        extension: {
          name: "test-extension",
          compilationMode: "PROLOGUE",
        },
      })
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.mkdirSync(path.join(projectDir, "includes"));

    fs.writeFileSync(
      path.join(projectDir, "package.json"),
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

    const compileResult = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir])
    );

    expect(compileResult.exitCode).equals(0);
    expect(compileResult.stdout).contains("Compiled 0 action(s).");
  });
});
