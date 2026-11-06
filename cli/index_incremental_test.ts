import { expect } from "chai";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import {
  CREDENTIALS_PATH,
  INTEGRATION_TEST_LOCATION,
  INTEGRATION_TEST_PROJECT,
  runCli,
  setupProject,
} from "df/cli/index_test_base";
import { suite, test, writeDefinitionFile } from "df/testing";
import { dropDatasets, dropStaleDatasets, makeUniqueSuffix } from "df/testing/bigquery";
import { integrationSuite } from "df/testing/credentials";
import { TmpDirFixture } from "df/testing/fixtures";

const ON_SCHEMA_CHANGE_DATASET = "dataform_e2e_osc";

integrationSuite("incremental", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

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
    });

    test(
      "executes EXTEND strategy in BigQuery and populates dynamically added columns",
      { timeout: 120000 },
      async () => {
        writeDefinitionFile(
          projectDir,
          "example_incremental.sqlx",
          `
config { 
  type: "incremental",
  onSchemaChange: "EXTEND"
}
\${when(
  incremental(),
  "SELECT 1 AS id, 'new' AS field1, 'new2' AS field2",
  "SELECT 1 AS id, 'old' AS field1"
)}
`,
        );

        writeDefinitionFile(
          projectDir,
          "verify_extend.sqlx",
          `
config {
  type: "assertion"
}
SELECT * FROM \${ref("example_incremental")}
WHERE (field1 = 'new' AND (field2 IS NULL OR field2 != 'new2'))
   OR (SELECT COUNT(*) FROM \${ref("example_incremental")}) != 2
`,
        );

        writeDefinitionFile(
          projectDir,
          "example_extend_remove.sqlx",
          `
config {
  type: "incremental",
  onSchemaChange: "EXTEND"
}
\${when(
  incremental(),
  "SELECT 1 AS id",
  "SELECT 1 AS id, 'old' AS field1"
)}
`,
        );

        // Initial run creates the dataset and populates the initial tables.
        const initialRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--actions=example_incremental,example_extend_remove",
        ]);
        expect(
          initialRunResult.exitCode,
          `stdout:\n${initialRunResult.stdout}\nstderr:\n${initialRunResult.stderr}`,
        ).equals(0);

        // Run the incremental table in dry-run mode first to verify procedural SQL structure.
        const dryRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--dry-run",
          "--json",
          "--actions=example_incremental",
        ]);

        expect(dryRunResult.exitCode).equals(0);
        const executionGraph = JSON.parse(dryRunResult.stdout);
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
        expect(statement).to.include("EXECUTE IMMEDIATE");

        // Execute EXTEND live against BigQuery and verify the new column is populated.
        const incrementalRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--actions=example_incremental,verify_extend",
        ]);
        expect(
          incrementalRunResult.exitCode,
          `stdout:\n${incrementalRunResult.stdout}\nstderr:\n${incrementalRunResult.stderr}`,
        ).equals(0);

        // Verify EXTEND raises an error when a column is removed.
        const removeColumnResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--actions=example_extend_remove",
        ]);
        expect(removeColumnResult.exitCode).equals(1);
        expect(removeColumnResult.stderr).to.include(
          "Column removals are not allowed when on_schema_change = 'EXTEND'.",
        );
      },
    );

    test(
      "executes SYNCHRONIZE strategy in BigQuery and drops removed columns while adding new ones",
      { timeout: 120000 },
      async () => {
        writeDefinitionFile(
          projectDir,
          "example_synchronize.sqlx",
          `
config {
  type: "incremental",
  uniqueKey: ["id"],
  onSchemaChange: "SYNCHRONIZE"
}
\${when(
  incremental(),
  "SELECT 1 AS id, 'synced' AS field2",
  "SELECT 1 AS id, 'old' AS field1"
)}
`,
        );

        writeDefinitionFile(
          projectDir,
          "verify_synchronize.sqlx",
          `
config {
  type: "assertion"
}
SELECT * FROM \${ref("example_synchronize")}
WHERE id != 1 OR field2 IS NULL OR field2 != 'synced'
   OR (SELECT COUNT(*) FROM \${ref("example_synchronize")}) != 1
`,
        );

        const initialRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--actions=example_synchronize",
        ]);
        expect(
          initialRunResult.exitCode,
          `stdout:\n${initialRunResult.stdout}\nstderr:\n${initialRunResult.stderr}`,
        ).equals(0);

        // Execute SYNCHRONIZE live against BigQuery (drops field1, adds field2) and verify result.
        const syncRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
        ]);
        expect(
          syncRunResult.exitCode,
          `stdout:\n${syncRunResult.stdout}\nstderr:\n${syncRunResult.stderr}`,
        ).equals(0);
      },
    );

    test(
      "executes FAIL strategy in BigQuery and raises an error on schema mismatch",
      { timeout: 120000 },
      async () => {
        writeDefinitionFile(
          projectDir,
          "example_fail.sqlx",
          `
config {
  type: "incremental",
  onSchemaChange: "FAIL"
}
\${when(
  incremental(),
  "SELECT 1 AS id, 'new' AS field1, 'new2' AS field2",
  "SELECT 1 AS id, 'old' AS field1"
)}
`,
        );

        const initialRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--actions=example_fail",
        ]);
        expect(
          initialRunResult.exitCode,
          `stdout:\n${initialRunResult.stdout}\nstderr:\n${initialRunResult.stderr}`,
        ).equals(0);

        const failRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--actions=example_fail",
        ]);
        expect(failRunResult.exitCode).equals(1);
        expect(failRunResult.stderr).to.include(
          "Schema mismatch defined by on_schema_change = 'FAIL'.",
        );
      },
    );

    test(
      "executes INSERT_OVERWRITE incremental strategy with EXTEND in BigQuery",
      { timeout: 120000 },
      async () => {
        writeDefinitionFile(
          projectDir,
          "example_insert_overwrite.sqlx",
          `
config {
  type: "incremental",
  onSchemaChange: "EXTEND",
  incrementalStrategy: "INSERT_OVERWRITE",
  bigquery: {
    partitionBy: "DATE(ts)"
  }
}
\${when(
  incremental(),
  "SELECT TIMESTAMP('2024-01-02 12:00:00 UTC') AS ts, 2 AS id, 'overwritten' AS field1, 'added' AS field2",
  "SELECT TIMESTAMP('2024-01-01 00:00:00 UTC') AS ts, 1 AS id, 'keep' AS field1 UNION ALL SELECT TIMESTAMP('2024-01-02 00:00:00 UTC') AS ts, 2 AS id, 'overwrite_me' AS field1"
)}
`,
        );

        writeDefinitionFile(
          projectDir,
          "verify_insert_overwrite.sqlx",
          `
config {
  type: "assertion"
}
SELECT * FROM \${ref("example_insert_overwrite")}
WHERE NOT (
  (id = 1 AND field1 = 'keep' AND field2 IS NULL) OR
  (id = 2 AND field1 = 'overwritten' AND field2 = 'added')
)
OR (SELECT COUNT(*) FROM \${ref("example_insert_overwrite")}) != 2
`,
        );

        const initialRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--actions=example_insert_overwrite",
        ]);
        expect(
          initialRunResult.exitCode,
          `stdout:\n${initialRunResult.stdout}\nstderr:\n${initialRunResult.stderr}`,
        ).equals(0);

        const incrementalRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
        ]);
        expect(
          incrementalRunResult.exitCode,
          `stdout:\n${incrementalRunResult.stdout}\nstderr:\n${incrementalRunResult.stderr}`,
        ).equals(0);
      },
    );

    test(
      "executes pre_operations and post_operations in the same scope as onSchemaChange incremental query",
      { timeout: 120000 },
      async () => {
        writeDefinitionFile(
          projectDir,
          "example_pre_post_ops.sqlx",
          `
config {
  type: "incremental",
  onSchemaChange: "EXTEND"
}

pre_operations {
  DECLARE pre_op_var STRING DEFAULT 'from_pre_op';
}

\${when(
  incremental(),
  "SELECT 2 AS id, pre_op_var AS field1, 'added_in_query' AS field2",
  "SELECT 1 AS id, 'initial' AS field1"
)}

post_operations {
  \${when(
    incremental(),
    \`INSERT INTO \${self()} (id, field1, field2) VALUES (3, pre_op_var, 'added_in_post_op')\`
  )}
}
`,
        );

        writeDefinitionFile(
          projectDir,
          "verify_pre_post_ops.sqlx",
          `
config {
  type: "assertion"
}
SELECT * FROM \${ref("example_pre_post_ops")}
WHERE NOT (
  (id = 1 AND field1 = 'initial' AND field2 IS NULL) OR
  (id = 2 AND field1 = 'from_pre_op' AND field2 = 'added_in_query') OR
  (id = 3 AND field1 = 'from_pre_op' AND field2 = 'added_in_post_op')
)
OR (SELECT COUNT(*) FROM \${ref("example_pre_post_ops")}) != 3
`,
        );

        // Initial run creates the table with (id, field1).
        const initialRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
          "--actions=example_pre_post_ops",
        ]);
        expect(
          initialRunResult.exitCode,
          `stdout:\n${initialRunResult.stdout}\nstderr:\n${initialRunResult.stderr}`,
        ).equals(0);

        // Incremental run alters the table to add field2, populates it using pre_op_var in both
        // the incremental query and post_operations, and verifies the resulting rows.
        const incrementalRunResult = await runCli("run", [
          projectDir,
          "--credentials",
          CREDENTIALS_PATH,
        ]);
        expect(
          incrementalRunResult.exitCode,
          `stdout:\n${incrementalRunResult.stdout}\nstderr:\n${incrementalRunResult.stderr}`,
        ).equals(0);
      },
    );
  });
});
