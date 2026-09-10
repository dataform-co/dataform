import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import {
  alterWorkflowSettings,
  CREDENTIALS_PATH,
  INTEGRATION_TEST_LOCATION,
  INTEGRATION_TEST_PROJECT,
  INTEGRATION_TEST_RESERVATION,
  runCli,
  setupProject
} from "df/cli/index_test_base";
import { version } from "df/core/version";
import { suite, test, writeDefinitionFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("run e2e", ({ afterEach }) => {
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
`
    );

    // Compile the project using the CLI.
    const compileResult = await runCli(
      "compile", 
      [
        projectDir,
        "--json",
        "--vars=testVar1=testValue1,testVar2=testValue2",
        "--schema-suffix=test_schema_suffix"
      ]
    );

    expect(compileResult.exitCode).equals(0);

    expect(JSON.parse(compileResult.stdout)).deep.equals({
      tables: [
        {
          type: "table",
          enumType: "TABLE",
          target: {
            database: INTEGRATION_TEST_PROJECT,
            schema: "dataform_test_schema_suffix",
            name: "example"
          },
          canonicalTarget: {
            schema: "dataform",
            name: "example",
            database: INTEGRATION_TEST_PROJECT
          },
          query: "\n\nselect 1 as testValue2\n",
          disabled: false,
          fileName: "definitions/example.sqlx",
          hermeticity: "NON_HERMETIC",
          tags: ["someTag"]
        }
      ],
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        vars: {
          testVar1: "testValue1",
          testVar2: "testValue2"
        },
        schemaSuffix: "test_schema_suffix"
      },
      graphErrors: {},
      jitData: {},
      dataformCoreVersion: version,
      targets: [
        {
          database: INTEGRATION_TEST_PROJECT,
          schema: "dataform",
          name: "example"
        }
      ]
    });

    // Dry run the project.
    const runResult = await runCli(
      "run", 
      [
        projectDir,
        "--credentials",
        CREDENTIALS_PATH,
        "--dry-run",
        "--json",
        "--vars=testVar1=testValue1,testVar2=testValue2",
        "--default-location=europe",
        "--tags=someTag,someOtherTag",
        "--actions=example,someOtherAction"
      ]
    );

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
            schema: "dataform"
          },
          tasks: [
            {
              statement:
                `create or replace table \`${INTEGRATION_TEST_PROJECT}.dataform.example\` as \n\nselect 1 as testValue2`,
              type: "statement"
            }
          ],
          type: "table"
        }
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
          testVar2: "testValue2"
        }
      },
      runConfig: {
        fullRefresh: false,
        tags: ["someTag", "someOtherTag"],
        actions: ["example", "someOtherAction"]
      },
      warehouseState: {}
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
`
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
`
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
            schema: "dataform"
          },
          tasks: [
            {
              statement:
                `create or replace table \`${INTEGRATION_TEST_PROJECT}.dataform.example_table\` as \n\nSELECT 1 as id`,
              type: "statement"
            }
          ],
          type: "table"
        },
        {
          fileName: "definitions/test_assertion.sqlx",
          hermeticity: "HERMETIC",
          target: {
            database: INTEGRATION_TEST_PROJECT,
            name: "test_assertion",
            schema: "dataform_assertions"
          },
          type: "assertion"
        }
      ],
      jitData: {},
      projectConfig: {
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        defaultSchema: "dataform",
        disableAssertions: true,
        warehouse: "bigquery"
      },
      runConfig: {
        actions: ["test_assertion", "example_table"],
        fullRefresh: false
      },
      warehouseState: {}
    };

    test("with --disable-assertions flag", async () => {
      alterWorkflowSettings(projectDir, { disableAssertions: false });

      const runResult = await runCli(
        "run",
        [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          "--disable-assertions",
          "--actions=test_assertion,example_table"
        ]
      );

      if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
        console.error("ASSERTIONS TEST FAILED. STDERR:", runResult.stderr);
      }
      expect(runResult.exitCode).equals(0);
      expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
    });

    test("with disableAssertions set in workflow_settings.yaml", async () => {
      alterWorkflowSettings(projectDir, { disableAssertions: true });

      const runResult = await runCli(
        "run",
        [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          "--actions=test_assertion,example_table"
        ]
      );

      if (runResult.exitCode !== 0 || runResult.stdout.trim().length === 0) {
        console.error("ASSERTIONS TEST FAILED. STDERR:", runResult.stderr);
      }
      expect(runResult.exitCode).equals(0);
      expect(JSON.parse(runResult.stdout)).deep.equals(expectedRunResult);
    });

    test("with --job-labels flag", async () => {
      alterWorkflowSettings(projectDir, { disableAssertions: false });

      const runResult = await runCli(
        "run",
        [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          "--disable-assertions",
          "--actions=test_assertion,example_table",
          "--job-labels=env=testing,team=dataform"
        ]
      );

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
`
      );
    });

    test("--default-reservation flag is applied to projectConfig in compile output", async () => {
      const compileResult = await runCli(
        "compile",
        [
          projectDir,
          "--json",
          `--default-reservation=${INTEGRATION_TEST_RESERVATION}`
        ]
      );

      expect(compileResult.exitCode).equals(0);
      const compiledGraph = JSON.parse(compileResult.stdout);
      expect(compiledGraph.projectConfig).deep.equals({
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        defaultReservation: INTEGRATION_TEST_RESERVATION
      });
    });

    test("--default-reservation flag is applied to projectConfig in run (dry-run) output", async () => {
      const runResult = await runCli(
        "run",
        [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          `--default-reservation=${INTEGRATION_TEST_RESERVATION}`,
          "--actions=example_table"
        ]
      );

      expect(runResult.exitCode).equals(0);
      const executionGraph = JSON.parse(runResult.stdout);
      expect(executionGraph.projectConfig).deep.equals({
        warehouse: "bigquery",
        defaultSchema: "dataform",
        assertionSchema: "dataform_assertions",
        defaultDatabase: INTEGRATION_TEST_PROJECT,
        defaultLocation: INTEGRATION_TEST_LOCATION,
        defaultReservation: INTEGRATION_TEST_RESERVATION
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
`
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
`
      );

      // Run tests using the CLI.
      const testResult = await runCli(
        "test",
        [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--json"
        ]
      );

      expect(testResult.exitCode).equals(0);

      expect(JSON.parse(testResult.stdout)).deep.equals([    {
        "name": "example_test",
        "successful": true,
      }]);
  });

    test("golden with failed unit test", async () => {
      // Write a simple failing test to the project.
      writeDefinitionFile(
        projectDir,
        "example_test.sqlx",
        `
config { type: "test", dataset: "example" }
select 2
`
      );

      // Run tests using the CLI.
      const testResult = await runCli(
        "test",
        [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--json"
        ]
      );

      expect(testResult.exitCode).equals(1);

      expect(JSON.parse(testResult.stdout)).deep.equals([{
        "name": "example_test",
        "successful": false,
        messages: [
          "For row 0 and column \"f0_\": expected \"2\", but saw \"1\"."
        ]
      }]);
    });

  });

  suite("onSchemaChange", ({ beforeEach }) => {
    let projectDir: string;
    const uniqueDataset = `dataform_e2e_osc_${Math.random().toString(36).substring(7)}`;

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
`
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
`
      );

      writeDefinitionFile(
        projectDir,
        "teardown_schema.sqlx",
        `
config { 
  type: "operations"
}
DROP SCHEMA IF EXISTS \`\${dataform.projectConfig.defaultDatabase}.\${dataform.projectConfig.defaultSchema}\` CASCADE
`
      );
    });

    test("generates dynamic SQL for EXTEND when table exists in BigQuery", { timeout: 120000 }, async () => {
      try {
        // Run setup operation to create the table in BigQuery.
        // Dataform will automatically create the uniqueDataset schema.
        await runCli(
          "run",
          [
            projectDir,
            "--credentials",
            CREDENTIALS_PATH,
            "--actions=setup_table"
          ]
        );

        // Run the incremental table in dry-run mode. 
        // Dataform will detect the table exists and generate the dynamic procedural SQL.
        const runResult = await runCli(
          "run",
          [
            projectDir,
            "--credentials",
            CREDENTIALS_PATH,
            "--dry-run",
            "--json",
            "--actions=example_incremental"
          ]
        );

        expect(runResult.exitCode).equals(0);
        const executionGraph = JSON.parse(runResult.stdout);
        const statement = executionGraph.actions[0].tasks[0].statement;

        const expectedRunResult = {
          projectConfig: {
            warehouse: "bigquery",
            defaultSchema: uniqueDataset,
            assertionSchema: "dataform_assertions",
            defaultDatabase: INTEGRATION_TEST_PROJECT,
            defaultLocation: INTEGRATION_TEST_LOCATION
          },
          runConfig: {
            actions: ["example_incremental"],
            fullRefresh: false
          },
          actions: [
            {
              fileName: "definitions/example_incremental.sqlx",
              hermeticity: "NON_HERMETIC",
              tableType: "incremental",
              target: {
                database: INTEGRATION_TEST_PROJECT,
                name: "example_incremental",
                schema: uniqueDataset
              },
              tasks: [
                {
                  statement,
                  type: "statement"
                }
              ],
              type: "table"
            }
          ],
          jitData: {},
          warehouseState: executionGraph.warehouseState
        };
        
        expect(executionGraph).deep.equals(expectedRunResult);
        expect(statement).to.include("CREATE OR REPLACE PROCEDURE");
        expect(statement).to.include("Column removals are not allowed when on_schema_change = 'EXTEND'.");
        expect(statement).to.include("ALTER TABLE");
        expect(statement).to.include("ADD COLUMN IF NOT EXISTS");
      } finally {
        // Teardown the schema completely, regardless of test success or failure.
        await runCli(
          "run",
          [
            projectDir,
            "--credentials",
            CREDENTIALS_PATH,
            "--actions=teardown_schema"
          ]
        );
      }
    });
  });

  suite("run --timeout deprecation", ({ beforeEach }) => {
    let projectDir: string;

    beforeEach("setup test project", () => {
      projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `defaultProject: ${INTEGRATION_TEST_PROJECT}\ndefaultLocation: ${INTEGRATION_TEST_LOCATION}\n`
      );
    });

    test("--timeout on run emits deprecation notice pointing to --execution-timeout", async () => {
      // The notice fires in the run handler before compile. Yargs validation
      // requires workflow_settings.yaml to exist, but compile can fail after that
      // — we only assert on stderr for the notice line.
      const runResult = await runCli(
        "run",
        [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--timeout",
          "30s"
        ]
      );

      expect(runResult.stderr).to.match(
        /--timeout only bounds project compilation[\s\S]*use --execution-timeout/
      );
    });

    test("--timeout on run does NOT emit notice when --execution-timeout is also set", async () => {
      const runResult = await runCli(
        "run",
        [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--timeout",
          "30s",
          "--execution-timeout",
          "10m"
        ]
      );

      expect(runResult.stderr).to.not.match(
        /--timeout only bounds project compilation/
      );
    });
  })
});
