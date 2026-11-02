import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import {
  alterWorkflowSettings,
  CREDENTIALS_PATH,
  INTEGRATION_TEST_LOCATION,
  INTEGRATION_TEST_PROJECT,
  INTEGRATION_TEST_RESERVATION,
  runCli,
  setupProject,
} from "df/cli/index_test_base";
import { version } from "df/core/version";
import { suite, test, writeDefinitionFile } from "df/testing";
import { dropDatasets, dropStaleDatasets, makeUniqueSuffix } from "df/testing/bigquery";
import { integrationSuite, readTestCredentialsConfig } from "df/testing/credentials";
import { TmpDirFixture } from "df/testing/fixtures";

const ON_SCHEMA_CHANGE_DATASET = "dataform_e2e_osc";
const LOCATION_RESOLUTION_DATASET = "dataform_e2e_loc";
const OPERATIONS_DATASET = "dataform_e2e_ops";

integrationSuite("run e2e", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("golden path with package.json", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    await setupProject(tmpDirFixture, projectDir);

    // Write a simple file to the project.
    writeDefinitionFile(
      projectDir,
      "example.sqlx",
      `
config { type: "table", tags: ["someTag"] }
select 1 as \${dataform.projectConfig.vars.testVar2}
`,
    );

    // Compile the project using the CLI.
    const compileResult = await runCli("compile", [
      projectDir,
      "--json",
      "--vars=testVar1=testValue1,testVar2=testValue2",
      "--schema-suffix=test_schema_suffix",
    ]);

    expect(compileResult.exitCode).equals(0);

    expect(JSON.parse(compileResult.stdout)).deep.equals({
      tables: [
        {
          type: "table",
          enumType: "TABLE",
          target: {
            database: INTEGRATION_TEST_PROJECT,
            schema: "dataform_test_schema_suffix",
            name: "example",
          },
          canonicalTarget: {
            schema: "dataform",
            name: "example",
            database: INTEGRATION_TEST_PROJECT,
          },
          query: "\n\nselect 1 as testValue2\n",
          disabled: false,
          fileName: "definitions/example.sqlx",
          hermeticity: "NON_HERMETIC",
          tags: ["someTag"],
        },
      ],
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        vars: {
          testVar1: "testValue1",
          testVar2: "testValue2",
        },
        schemaSuffix: "test_schema_suffix",
      },
      graphErrors: {},
      jitData: {},
      dataformCoreVersion: version,
      targets: [
        {
          database: INTEGRATION_TEST_PROJECT,
          schema: "dataform",
          name: "example",
        },
      ],
    });

    // Dry run the project.
    const runResult = await runCli("run", [
      projectDir,
      "--credentials",
      CREDENTIALS_PATH,
      "--dry-run",
      "--json",
      "--vars=testVar1=testValue1,testVar2=testValue2",
      "--default-location=europe",
      "--tags=someTag,someOtherTag",
      "--actions=example,someOtherAction",
    ]);

    if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
      console.error("GOLDEN PATH FAILED. STDERR:", runResult.stderr);
    }
    expect(runResult.exitCode).equals(0);

    expect(JSON.parse(runResult.stdout)).deep.equals({
      actions: [
        {
          fileName: "definitions/example.sqlx",
          hermeticity: "NON_HERMETIC",
          tableType: "table",
          target: {
            database: INTEGRATION_TEST_PROJECT,
            name: "example",
            schema: "dataform",
          },
          tasks: [
            {
              statement: `create or replace table \`${INTEGRATION_TEST_PROJECT}.dataform.example\` as \n\nselect 1 as testValue2`,
              type: "statement",
            },
          ],
          type: "table",
        },
      ],
      jitData: {},
      projectConfig: {
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: "europe",
        defaultSchema: "dataform",
        warehouse: "bigquery",
        vars: {
          testVar1: "testValue1",
          testVar2: "testValue2",
        },
      },
      runConfig: {
        fullRefresh: false,
        tags: ["someTag", "someOtherTag"],
        actions: ["example", "someOtherAction"],
      },
      warehouseState: {},
    });
  });

  suite("disable-assertions flag (run)", ({ beforeEach }) => {
    let projectDir: string;

    beforeEach("setup test project", async () => {
      projectDir = tmpDirFixture.createNewTmpDir();
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
    });

    const expectedRunResult = {
      actions: [
        {
          fileName: "definitions/example_table.sqlx",
          hermeticity: "NON_HERMETIC",
          tableType: "table",
          target: {
            database: INTEGRATION_TEST_PROJECT,
            name: "example_table",
            schema: "dataform",
          },
          tasks: [
            {
              statement: `create or replace table \`${INTEGRATION_TEST_PROJECT}.dataform.example_table\` as \n\nSELECT 1 as id`,
              type: "statement",
            },
          ],
          type: "table",
        },
        {
          fileName: "definitions/test_assertion.sqlx",
          hermeticity: "HERMETIC",
          target: {
            database: INTEGRATION_TEST_PROJECT,
            name: "test_assertion",
            schema: "dataform_assertions",
          },
          type: "assertion",
        },
      ],
      jitData: {},
      projectConfig: {
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        defaultSchema: "dataform",
        disableAssertions: true,
        warehouse: "bigquery",
      },
      runConfig: {
        actions: ["test_assertion", "example_table"],
        fullRefresh: false,
      },
      warehouseState: {},
    };

    test("with --disable-assertions flag", async () => {
      alterWorkflowSettings(projectDir, { disableAssertions: false });

      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--disable-assertions",
        "--actions=test_assertion,example_table",
      ]);

      if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
        console.error("ASSERTIONS TEST FAILED. STDERR:", runResult.stderr);
      }
      expect(runResult.exitCode).equals(0);
      expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
    });

    test("with disableAssertions set in workflow_settings.yaml", async () => {
      alterWorkflowSettings(projectDir, { disableAssertions: true });

      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--actions=test_assertion,example_table",
      ]);

      if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
        console.error("ASSERTIONS TEST FAILED. STDERR:", runResult.stderr);
      }
      expect(runResult.exitCode).equals(0);
      expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
    });

    test("with --job-labels flag", async () => {
      alterWorkflowSettings(projectDir, { disableAssertions: false });

      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--disable-assertions",
        "--actions=test_assertion,example_table",
        "--job-labels=env=testing,team=dataform",
      ]);

      if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
        console.error("ASSERTIONS TEST FAILED. STDERR:", runResult.stderr);
      }
      expect(runResult.exitCode).equals(0);
      expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
    });
  });

  suite("--default-reservation flag", ({ beforeEach }) => {
    let projectDir: string;

    beforeEach("setup test project", async () => {
      projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);

      writeDefinitionFile(
        projectDir,
        "example_table.sqlx",
        `
config { type: "table" }
SELECT 1 as id
`,
      );
    });

    test("--default-reservation flag is applied to projectConfig in compile output", async () => {
      const compileResult = await runCli("compile", [
        projectDir,
        "--json",
        `--default-reservation=${INTEGRATION_TEST_RESERVATION}`,
      ]);

      expect(compileResult.exitCode).equals(0);
      const compiledGraph = JSON.parse(compileResult.stdout);
      expect(compiledGraph.projectConfig).deep.equals({
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        defaultReservation: INTEGRATION_TEST_RESERVATION,
      });
    });

    test("--default-reservation flag is applied to projectConfig in run (dry-run) output", async () => {
      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        `--default-reservation=${INTEGRATION_TEST_RESERVATION}`,
        "--actions=example_table",
      ]);

      expect(runResult.exitCode).equals(0);
      const executionGraph = JSON.parse(runResult.stdout);
      expect(executionGraph.projectConfig).deep.equals({
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        defaultReservation: INTEGRATION_TEST_RESERVATION,
      });
    });
  });

  suite("unit tests", ({ beforeEach }) => {
    let projectDir: string;

    beforeEach("setup test project", async () => {
      projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir);
      // Write a simple file to the project.
      writeDefinitionFile(
        projectDir,
        "example.sqlx",
        `
config { type: "table" }
select 1
`,
      );
    });

    test("golden with successful unit test", async () => {
      // Write a simple passing test to the project.
      writeDefinitionFile(
        projectDir,
        "example_test.sqlx",
        `
config { type: "test", dataset: "example" }
select 1
`,
      );

      // Run tests using the CLI.
      const testResult = await runCli("test", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--json",
      ]);

      expect(testResult.exitCode).equals(0);

      expect(JSON.parse(testResult.stdout)).deep.equals([
        {
          name: "example_test",
          successful: true,
        },
      ]);
    });

    test("golden with failed unit test", async () => {
      // Write a simple failing test to the project.
      writeDefinitionFile(
        projectDir,
        "example_test.sqlx",
        `
config { type: "test", dataset: "example" }
select 2
`,
      );

      // Run tests using the CLI.
      const testResult = await runCli("test", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--json",
      ]);

      expect(testResult.exitCode).equals(1);

      expect(JSON.parse(testResult.stdout)).deep.equals([
        {
          name: "example_test",
          successful: false,
          messages: ['For row 0 and column "f0_": expected "2", but saw "1".'],
        },
      ]);
    });
  });

  suite("onSchemaChange", ({ before, beforeEach, after }) => {
    let projectDir: string;
    const dbadapter = new BigQueryDbAdapter(dfapi.credentials.read(CREDENTIALS_PATH));
    const uniqueDataset = `${ON_SCHEMA_CHANGE_DATASET}_${makeUniqueSuffix()}`;

    before("drop stale datasets", async () => {
      await dropStaleDatasets(dbadapter, {
        project: INTEGRATION_TEST_PROJECT,
        location: INTEGRATION_TEST_LOCATION,
        prefixes: [ON_SCHEMA_CHANGE_DATASET],
      });
    });

    after("drop datasets", async () => {
      await dropDatasets(dbadapter, INTEGRATION_TEST_PROJECT, [uniqueDataset]);
    });

    beforeEach("setup test project", async () => {
      projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir, { defaultDataset: uniqueDataset });

      writeDefinitionFile(
        projectDir,
        "setup_table.sqlx",
        `
config { 
  type: "operations"
}
CREATE OR REPLACE TABLE \`\${dataform.projectConfig.defaultDatabase}.\${dataform.projectConfig.defaultSchema}.example_incremental\` AS SELECT 1 AS id, 'old' AS field1
`,
      );

      writeDefinitionFile(
        projectDir,
        "example_incremental.sqlx",
        `
config { 
  type: "incremental",
  onSchemaChange: "EXTEND"
}
SELECT 1 as id, 'new' as field1, 'new2' as field2
`,
      );

      writeDefinitionFile(
        projectDir,
        "teardown_schema.sqlx",
        `
config { 
  type: "operations"
}
DROP SCHEMA IF EXISTS \`\${dataform.projectConfig.defaultDatabase}.\${dataform.projectConfig.defaultSchema}\` CASCADE
`,
      );
    });

    test(
      "generates dynamic SQL for EXTEND when table exists in BigQuery",
      { timeout: 120000 },
      async () => {
        try {
          // Run setup operation to create the table in BigQuery.
          // Dataform will automatically create the uniqueDataset schema.
          await runCli("run", [
            projectDir,
            "--credentials",
            CREDENTIALS_PATH,
            "--actions=setup_table",
          ]);

          // Run the incremental table in dry-run mode.
          // Dataform will detect the table exists and generate the dynamic procedural SQL.
          const runResult = await runCli("run", [
            projectDir,
            "--credentials",
            CREDENTIALS_PATH,
            "--dry-run",
            "--json",
            "--actions=example_incremental",
          ]);

          expect(runResult.exitCode).equals(0);
          const executionGraph = JSON.parse(runResult.stdout);
          const statement = executionGraph.actions[0].tasks[0].statement;

          const expectedRunResult = {
            projectConfig: {
              warehouse: "bigquery",
              defaultSchema: uniqueDataset,
              assertionSchema: "dataform_assertions",
              defaultDatabase: INTEGRATION_TEST_PROJECT,
              defaultLocation: INTEGRATION_TEST_LOCATION,
            },
            runConfig: {
              actions: ["example_incremental"],
              fullRefresh: false,
            },
            actions: [
              {
                fileName: "definitions/example_incremental.sqlx",
                hermeticity: "NON_HERMETIC",
                tableType: "incremental",
                target: {
                  database: INTEGRATION_TEST_PROJECT,
                  name: "example_incremental",
                  schema: uniqueDataset,
                },
                tasks: [
                  {
                    statement,
                    type: "statement",
                  },
                ],
                type: "table",
              },
            ],
            jitData: {},
            warehouseState: executionGraph.warehouseState,
          };

          expect(executionGraph).deep.equals(expectedRunResult);
          expect(statement).to.include("CREATE OR REPLACE PROCEDURE");
          expect(statement).to.include(
            "Column removals are not allowed when on_schema_change = 'EXTEND'.",
          );
          expect(statement).to.include("ALTER TABLE");
          expect(statement).to.include("ADD COLUMN IF NOT EXISTS");
        } finally {
          // Teardown the schema completely, regardless of test success or failure.
          await runCli("run", [
            projectDir,
            "--credentials",
            CREDENTIALS_PATH,
            "--actions=teardown_schema",
          ]);
        }
      },
    );
  });

  suite("operations", ({ before, after }) => {
    const dbadapter = new BigQueryDbAdapter(dfapi.credentials.read(CREDENTIALS_PATH));
    const uniqueDataset = `${OPERATIONS_DATASET}_${makeUniqueSuffix()}`;

    before("drop stale datasets", async () => {
      await dropStaleDatasets(dbadapter, {
        project: INTEGRATION_TEST_PROJECT,
        location: INTEGRATION_TEST_LOCATION,
        prefixes: [OPERATIONS_DATASET],
      });
    });

    after("drop datasets", async () => {
      await dropDatasets(dbadapter, INTEGRATION_TEST_PROJECT, [uniqueDataset]);
    });

    test("statements separated by --- run as a single script", { timeout: 120000 }, async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir, { defaultDataset: uniqueDataset });
      // The variable declared in the first statement is only visible to the second one if
      // both run in the same script.
      writeDefinitionFile(
        projectDir,
        "multi_statement_operation.sqlx",
        `
config { type: "operations" }
DECLARE min_id INT64 DEFAULT 0;
---
SELECT * FROM (SELECT 1 AS id) WHERE id >= min_id
`,
      );

      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--actions=multi_statement_operation",
      ]);

      expect(
        runResult.exitCode,
        `stdout:\n${runResult.stdout}\nstderr:\n${runResult.stderr}`,
      ).equals(0);
      expect(runResult.stdout).to.include("Operation completed successfully");
    });
  });

  suite("location resolution", ({ before, beforeEach, afterEach: suiteAfterEach }) => {
    // The expected location is never "US", which is the hardcoded fallback used when no location
    // is set anywhere, so the tests can't pass by accident.
    const EXPECTED_LOCATION = "EU";
    const OTHER_LOCATION = "US";
    const ALL_LOCATIONS = [EXPECTED_LOCATION, OTHER_LOCATION];

    const dbadapter = new BigQueryDbAdapter(dfapi.credentials.read(CREDENTIALS_PATH));
    const executorInLocation = (location: string) => ({
      execute: (statement: string) => dbadapter.execute(statement, { bigquery: { location } }),
    });

    let datasetsToDrop: string[];

    before("drop stale datasets", async () => {
      for (const location of ALL_LOCATIONS) {
        await dropStaleDatasets(executorInLocation(location), {
          project: INTEGRATION_TEST_PROJECT,
          location,
          prefixes: [LOCATION_RESOLUTION_DATASET],
        });
      }
    });

    beforeEach("reset datasets to drop", () => {
      datasetsToDrop = [];
    });

    suiteAfterEach("drop datasets", async () => {
      // A dataset may have ended up in the wrong location if a test failed, so try both.
      for (const location of ALL_LOCATIONS) {
        await dropDatasets(executorInLocation(location), INTEGRATION_TEST_PROJECT, datasetsToDrop);
      }
    });

    async function setupLocationProject(
      workflowSettingsLocation: string | undefined,
    ): Promise<{ projectDir: string; dataset: string }> {
      const dataset = `${LOCATION_RESOLUTION_DATASET}_${makeUniqueSuffix()}`;
      datasetsToDrop.push(dataset);

      const projectDir = tmpDirFixture.createNewTmpDir();
      await setupProject(tmpDirFixture, projectDir, { defaultDataset: dataset });

      const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
      const workflowSettings = loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")) as {
        [key: string]: any;
      };
      if (workflowSettingsLocation) {
        workflowSettings.defaultLocation = workflowSettingsLocation;
      } else {
        delete workflowSettings.defaultLocation;
      }
      fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));

      writeDefinitionFile(
        projectDir,
        "example.sqlx",
        `
config { type: "table" }
SELECT 1 AS id
`,
      );
      return { projectDir, dataset };
    }

    function writeCredentialsWithLocation(location: string): string {
      const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), "credentials.json");
      fs.writeFileSync(
        credentialsPath,
        JSON.stringify({ ...readTestCredentialsConfig(CREDENTIALS_PATH), location }),
      );
      return credentialsPath;
    }

    async function runProject(projectDir: string, credentialsPath: string, args: string[] = []) {
      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        credentialsPath,
        ...args,
      ]);
      expect(
        runResult.exitCode,
        `stdout:\n${runResult.stdout}\nstderr:\n${runResult.stderr}`,
      ).equals(0);
    }

    async function expectDatasetLocation(dataset: string, expectedLocation: string) {
      // INFORMATION_SCHEMA.SCHEMATA only lists datasets in the queried region.
      const { rows } = await dbadapter.execute(
        `select location
         from \`${INTEGRATION_TEST_PROJECT}\`.\`region-${expectedLocation.toLowerCase()}\`.INFORMATION_SCHEMA.SCHEMATA
         where schema_name = '${dataset}'`,
        { bigquery: { location: expectedLocation } },
      );
      expect(rows.length, `dataset ${dataset} not found in ${expectedLocation}`).equals(1);
      expect(rows[0].location.toUpperCase()).equals(expectedLocation.toUpperCase());
    }

    test(
      "--default-location overrides workflow_settings.yaml and credentials",
      { timeout: 120000 },
      async () => {
        const { projectDir, dataset } = await setupLocationProject(OTHER_LOCATION);

        await runProject(projectDir, writeCredentialsWithLocation(OTHER_LOCATION), [
          `--default-location=${EXPECTED_LOCATION}`,
        ]);

        await expectDatasetLocation(dataset, EXPECTED_LOCATION);
      },
    );

    test(
      "workflow_settings.yaml defaultLocation takes precedence over credentials",
      { timeout: 120000 },
      async () => {
        const { projectDir, dataset } = await setupLocationProject(EXPECTED_LOCATION);

        await runProject(projectDir, writeCredentialsWithLocation(OTHER_LOCATION));

        await expectDatasetLocation(dataset, EXPECTED_LOCATION);
      },
    );

    test(
      "falls back to credentials location when not set in arguments or workflow_settings.yaml",
      { timeout: 120000 },
      async () => {
        const { projectDir, dataset } = await setupLocationProject(undefined);

        await runProject(projectDir, writeCredentialsWithLocation(EXPECTED_LOCATION));

        await expectDatasetLocation(dataset, EXPECTED_LOCATION);
      },
    );
  });

  suite("run --timeout deprecation", ({ beforeEach }) => {
    let projectDir: string;

    beforeEach("setup test project", () => {
      projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `defaultProject: ${INTEGRATION_TEST_PROJECT}\ndefaultLocation: ${INTEGRATION_TEST_LOCATION}\n`,
      );
    });

    test("--timeout on run emits deprecation notice pointing to --execution-timeout", async () => {
      // The notice fires in the run handler before compile. Yargs validation
      // requires workflow_settings.yaml to exist, but compile can fail after that
      // — we only assert on stderr for the notice line.
      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--timeout",
        "30s",
      ]);

      expect(runResult.stderr).to.match(
        /--timeout only bounds project compilation[\s\S]*use --execution-timeout/,
      );
    });

    test("--timeout on run does NOT emit notice when --execution-timeout is also set", async () => {
      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--timeout",
        "30s",
        "--execution-timeout",
        "10m",
      ]);

      expect(runResult.stderr).to.not.match(/--timeout only bounds project compilation/);
    });
  });
});
