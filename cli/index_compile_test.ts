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
