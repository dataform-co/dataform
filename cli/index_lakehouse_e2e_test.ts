import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter, createBigQueryClientProvider } from "df/cli/api/dbadapters/bigquery";
import {
  CREDENTIALS_PATH,
  INTEGRATION_TEST_LAKEHOUSE_CATALOG,
  INTEGRATION_TEST_LAKEHOUSE_LOCATION,
  INTEGRATION_TEST_LAKEHOUSE_NAMESPACE,
  INTEGRATION_TEST_PROJECT,
  runCli,
  setupProject,
} from "df/cli/index_test_base";
import { dataform } from "df/protos/ts";
import { test, writeDefinitionFile } from "df/testing";
import { dropDatasets, dropStaleDatasets, makeUniqueSuffix } from "df/testing/bigquery";
import { integrationSuite, readTestCredentialsConfig } from "df/testing/credentials";
import { TmpDirFixture } from "df/testing/fixtures";

const SCRATCH_DATASET_PREFIXES = [
  "df_e2e_lh_table",
  "df_e2e_lh_inc",
  "df_e2e_lh_osc",
  "df_e2e_lh_op",
  "df_e2e_lh_meta",
  "df_e2e_lh_view",
  "df_e2e_lh_loc",
  "df_e2e_lh_ds_missing",
  "df_e2e_lh_ds_existing",
  "df_e2e_lh_missing_ns",
];

const STALE_TABLE_REGEX = /^lh_e2e_[a-z_]+_(\d+)_[0-9a-f]{8}$/;
const STALE_TABLE_MIN_AGE_HOURS = 6;

integrationSuite("lakehouse e2e", { parallel: true }, ({ before, after }) => {
  const tmpDirFixture = new TmpDirFixture(after);
  const credentials = dfapi.credentials.read(CREDENTIALS_PATH);
  const clientProvider = createBigQueryClientProvider(credentials);
  const dbadapter = new BigQueryDbAdapter(credentials, { clientProvider });
  const bigqueryClient = clientProvider(INTEGRATION_TEST_PROJECT);
  const lakehouseSchema = `${INTEGRATION_TEST_LAKEHOUSE_CATALOG}.${INTEGRATION_TEST_LAKEHOUSE_NAMESPACE}`;
  const lakehouseExecutor = {
    execute: (statement: string) =>
      dbadapter.execute(statement, {
        bigquery: { location: INTEGRATION_TEST_LAKEHOUSE_LOCATION },
      }),
  };

  function makeUniqueTableName(prefix: string): string {
    const epochSeconds = Math.floor(Date.now() / 1000);
    return `lh_e2e_${prefix}_${epochSeconds}_${makeUniqueSuffix()}`;
  }

  function writeCredentialsWithLocation(location: string | undefined): string {
    const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), "credentials.json");
    const config = { ...readTestCredentialsConfig(CREDENTIALS_PATH) };
    if (location === undefined) {
      delete config.location;
    } else {
      config.location = location;
    }
    fs.writeFileSync(credentialsPath, JSON.stringify(config));
    return credentialsPath;
  }

  before("verify Lakehouse namespace prerequisite and drop stale test resources", async () => {
    // Fail fast with an actionable message if the prerequisite Lakehouse catalog/namespace does not
    // exist in INTEGRATION_TEST_PROJECT or the caller lacks permissions (SQL cannot create
    // Lakehouse namespaces, and dbadapter.table() swallows 404).
    const probeTable = `lh_e2e_probe_${makeUniqueSuffix()}`;
    try {
      await dbadapter.execute(
        `CREATE TABLE \`${INTEGRATION_TEST_PROJECT}.${lakehouseSchema}.${probeTable}\` AS SELECT 1 AS id`,
        {
          bigquery: {
            dryRun: true,
            location: INTEGRATION_TEST_LAKEHOUSE_LOCATION,
          },
        },
      );
    } catch (e) {
      throw new Error(
        `Lakehouse E2E prerequisite check failed for '${INTEGRATION_TEST_PROJECT}.${lakehouseSchema}' ` +
          `(location '${INTEGRATION_TEST_LAKEHOUSE_LOCATION}'). Ensure the catalog and namespace ` +
          `exist or override INTEGRATION_TEST_LAKEHOUSE_CATALOG / INTEGRATION_TEST_LAKEHOUSE_NAMESPACE ` +
          `/ INTEGRATION_TEST_LAKEHOUSE_LOCATION (see contributing.md). Underlying error: ` +
          `${e?.message || e}`,
      );
    }

    await dropStaleDatasets(lakehouseExecutor, {
      project: INTEGRATION_TEST_PROJECT,
      location: INTEGRATION_TEST_LAKEHOUSE_LOCATION,
      prefixes: SCRATCH_DATASET_PREFIXES,
    });

    // Best-effort cleanup of orphaned Lakehouse tables left behind by killed/timed-out CI runs.
    try {
      const cutoffMillis = Date.now() - STALE_TABLE_MIN_AGE_HOURS * 3600 * 1000;
      const [existingTables] = await bigqueryClient
        .dataset(lakehouseSchema)
        .getTables({ autoPaginate: true, maxResults: 1000 });
      const staleTables = existingTables.filter((t) => {
        const match = STALE_TABLE_REGEX.exec(t.id || "");
        return match && parseInt(match[1], 10) * 1000 < cutoffMillis;
      });
      await Promise.all(
        staleTables.map((t) =>
          dbadapter
            .deleteTable(
              {
                database: INTEGRATION_TEST_PROJECT,
                schema: lakehouseSchema,
                name: t.id,
              },
              {
                bigquery: { location: INTEGRATION_TEST_LAKEHOUSE_LOCATION },
              },
            )
            .catch((err) => console.warn(`Failed to drop stale Lakehouse table ${t.id}: ${err}`)),
        ),
      );
    } catch (e) {
      console.warn(`Skipping stale Lakehouse table cleanup: ${e?.message || e}`);
    }
  });

  async function withLakehouseProject(
    datasetPrefix: string,
    fn: (ctx: {
      projectDir: string;
      scratchDataset: string;
      registerTableForCleanup: (tableName: string) => dataform.ITarget;
    }) => Promise<void>,
    options?: { defaultLocation?: string | undefined },
  ): Promise<void> {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const scratchDataset = `${datasetPrefix}_${makeUniqueSuffix()}`;
    const createdTables: dataform.ITarget[] = [];

    const registerTableForCleanup = (tableName: string): dataform.ITarget => {
      const target: dataform.ITarget = {
        database: INTEGRATION_TEST_PROJECT,
        schema: lakehouseSchema,
        name: tableName,
      };
      createdTables.push(target);
      return target;
    };

    const hasLocationOverride = options && "defaultLocation" in options;
    const effectiveLocation = hasLocationOverride
      ? options.defaultLocation
      : INTEGRATION_TEST_LAKEHOUSE_LOCATION;

    try {
      await setupProject(tmpDirFixture, projectDir, {
        defaultDataset: scratchDataset,
        ...(effectiveLocation ? { defaultLocation: effectiveLocation } : {}),
        defaultLakehouseConfig: {
          catalog: INTEGRATION_TEST_LAKEHOUSE_CATALOG,
          namespace: INTEGRATION_TEST_LAKEHOUSE_NAMESPACE,
        },
      });
      if (hasLocationOverride && effectiveLocation === undefined) {
        const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
        const workflowSettings = loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")) as {
          [key: string]: any;
        };
        delete workflowSettings.defaultLocation;
        fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));
      }

      await fn({ projectDir, scratchDataset, registerTableForCleanup });
    } finally {
      await Promise.all(
        createdTables.map(async (target) => {
          try {
            await dbadapter.deleteTable(target, {
              bigquery: { location: INTEGRATION_TEST_LAKEHOUSE_LOCATION },
            });
          } catch (e) {
            console.warn(`Failed to drop Lakehouse table ${target.name}: ${e}`);
          }
        }),
      );
      await dropDatasets(lakehouseExecutor, INTEGRATION_TEST_PROJECT, [scratchDataset]);
    }
  }

  async function runDataform(
    projectDir: string,
    extraArgs: string[] = [],
    credentialsPath: string = CREDENTIALS_PATH,
  ) {
    return await runCli("run", [
      projectDir,
      "--credentials",
      credentialsPath,
      "--json",
      ...extraArgs,
    ]);
  }

  async function selectAllLakehouseRows(tableName: string): Promise<any[]> {
    const { rows } = await dbadapter.execute(
      `SELECT * FROM \`${INTEGRATION_TEST_PROJECT}.${lakehouseSchema}.${tableName}\` ORDER BY id`,
      { bigquery: { location: INTEGRATION_TEST_LAKEHOUSE_LOCATION } },
    );
    return rows;
  }

  /**
   * In non-dry `dataform run --json` output, `Runner` only populates `task.compiledSql` when it
   * overrides the task's SQL (for example, wrapping it in `CREATE SCHEMA IF NOT EXISTS` for a
   * missing dataset). When `compiledSql` is `undefined`, the task executed unwrapped.
   */
  function expectUnwrappedTask(task: { compiledSql?: string } | undefined): void {
    expect(task, "expected executed task in run result").to.not.equal(undefined);
    expect(task?.compiledSql).equals(undefined);
  }

  function parseRunResultJson(res: { exitCode: number; stdout: string; stderr: string }): any {
    try {
      return JSON.parse(res.stdout);
    } catch (e) {
      throw new Error(
        `Expected JSON stdout from dataform CLI (exitCode=${res.exitCode}): ${e?.message || e}\n` +
          `stdout:\n${res.stdout}\nstderr:\n${res.stderr}`,
      );
    }
  }

  async function fetchExecutedJobQuery(jobId: string): Promise<string> {
    expect(jobId, "expected BigQuery jobId in task metadata").to.be.a("string").and.not.equal("");
    const [metadata] = await bigqueryClient
      .job(jobId, { location: INTEGRATION_TEST_LAKEHOUSE_LOCATION })
      .getMetadata();
    const query = String(metadata?.configuration?.query?.query || "");
    expect(query, `empty query for BigQuery job ${jobId}`).to.not.equal("");
    return query;
  }

  async function expectDatasetInLocation(dataset: string, expectedLocation: string): Promise<void> {
    const { rows } = await dbadapter.execute(
      `SELECT location
       FROM \`${INTEGRATION_TEST_PROJECT}\`.\`region-${expectedLocation.toLowerCase()}\`.INFORMATION_SCHEMA.SCHEMATA
       WHERE schema_name = @dataset`,
      {
        params: { dataset },
        bigquery: { location: expectedLocation },
      },
    );
    expect(rows.length, `dataset ${dataset} not found in ${expectedLocation}`).equals(1);
    expect(rows[0].location.toUpperCase()).equals(expectedLocation.toUpperCase());
  }

  // Case 1: `table` run twice, second run replaces the table.
  test(
    "table: creates and replaces a 4-part Lakehouse table across two runs",
    { timeout: 180000 },
    async () => {
      await withLakehouseProject(
        "df_e2e_lh_table",
        async ({ projectDir, registerTableForCleanup }) => {
          const tableName = makeUniqueTableName("table");
          registerTableForCleanup(tableName);

          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config { type: "table", lakehouse: {} }
SELECT 1 AS id, 'v1' AS val`,
          );
          const run1 = await runDataform(projectDir);
          expect(run1.exitCode, `stdout:\n${run1.stdout}\nstderr:\n${run1.stderr}`).equals(0);
          expect(await selectAllLakehouseRows(tableName)).to.eql([{ id: 1, val: "v1" }]);

          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config { type: "table", lakehouse: {} }
SELECT 1 AS id, 'v2' AS val
UNION ALL
SELECT 2 AS id, 'v2' AS val`,
          );
          const run2 = await runDataform(projectDir);
          expect(run2.exitCode, `stdout:\n${run2.stdout}\nstderr:\n${run2.stderr}`).equals(0);
          expect(await selectAllLakehouseRows(tableName)).to.eql([
            { id: 1, val: "v2" },
            { id: 2, val: "v2" },
          ]);
        },
      );
    },
  );

  // Case 2: `incremental` — first run creates, second without `uniqueKey` inserts, second with `uniqueKey` merges.
  test(
    "incremental: creates on first run, inserts without uniqueKey, and merges with uniqueKey",
    { timeout: 240000 },
    async () => {
      await withLakehouseProject(
        "df_e2e_lh_inc",
        async ({ projectDir, registerTableForCleanup }) => {
          const appendTable = makeUniqueTableName("inc_append");
          const mergeTable = makeUniqueTableName("inc_merge");
          registerTableForCleanup(appendTable);
          registerTableForCleanup(mergeTable);

          writeDefinitionFile(
            projectDir,
            `${appendTable}.sqlx`,
            `config { type: "incremental", lakehouse: {} }
SELECT 1 AS id, 'a' AS val`,
          );
          writeDefinitionFile(
            projectDir,
            `${mergeTable}.sqlx`,
            `config { type: "incremental", uniqueKey: ["id"], lakehouse: {} }
SELECT 1 AS id, 'v1' AS val
UNION ALL
SELECT 2 AS id, 'v1' AS val`,
          );

          const run1 = await runDataform(projectDir);
          expect(run1.exitCode, `stdout:\n${run1.stdout}\nstderr:\n${run1.stderr}`).equals(0);
          expect(await selectAllLakehouseRows(appendTable)).to.eql([{ id: 1, val: "a" }]);
          expect(await selectAllLakehouseRows(mergeTable)).to.eql([
            { id: 1, val: "v1" },
            { id: 2, val: "v1" },
          ]);

          writeDefinitionFile(
            projectDir,
            `${appendTable}.sqlx`,
            `config { type: "incremental", lakehouse: {} }
SELECT 2 AS id, 'b' AS val`,
          );
          writeDefinitionFile(
            projectDir,
            `${mergeTable}.sqlx`,
            `config { type: "incremental", uniqueKey: ["id"], lakehouse: {} }
SELECT 2 AS id, 'v2' AS val
UNION ALL
SELECT 3 AS id, 'v2' AS val`,
          );

          const run2 = await runDataform(projectDir);
          expect(run2.exitCode, `stdout:\n${run2.stdout}\nstderr:\n${run2.stderr}`).equals(0);

          expect(await selectAllLakehouseRows(appendTable)).to.eql([
            { id: 1, val: "a" },
            { id: 2, val: "b" },
          ]);
          expect(await selectAllLakehouseRows(mergeTable)).to.eql([
            { id: 1, val: "v1" },
            { id: 2, val: "v2" },
            { id: 3, val: "v2" },
          ]);
        },
      );
    },
  );

  // Case 3: `onSchemaChange`: EXTEND (new column) and SYNCHRONIZE (removed column) + no leftover scratch tables.
  test(
    "onSchemaChange: applies EXTEND and SYNCHRONIZE without leaving scratch tables in defaultDataset",
    { timeout: 240000 },
    async () => {
      await withLakehouseProject(
        "df_e2e_lh_osc",
        async ({ projectDir, scratchDataset, registerTableForCleanup }) => {
          const extendTable = makeUniqueTableName("osc_ext");
          const syncTable = makeUniqueTableName("osc_sync");
          const extendTarget = registerTableForCleanup(extendTable);
          const syncTarget = registerTableForCleanup(syncTable);

          // 3a. EXTEND: add new column `status`
          writeDefinitionFile(
            projectDir,
            `${extendTable}.sqlx`,
            `config { type: "incremental", onSchemaChange: "EXTEND", lakehouse: {} }
SELECT 1 AS id, 'alice' AS name`,
          );
          const extRun1 = await runDataform(projectDir, [`--actions=${extendTable}`]);
          expect(extRun1.exitCode, `stdout:\n${extRun1.stdout}\nstderr:\n${extRun1.stderr}`).equals(
            0,
          );

          writeDefinitionFile(
            projectDir,
            `${extendTable}.sqlx`,
            `config { type: "incremental", onSchemaChange: "EXTEND", lakehouse: {} }
SELECT 2 AS id, 'bob' AS name, 'active' AS status`,
          );
          const extRun2 = await runDataform(projectDir, [`--actions=${extendTable}`]);
          expect(extRun2.exitCode, `stdout:\n${extRun2.stdout}\nstderr:\n${extRun2.stderr}`).equals(
            0,
          );

          const extendMetadata = await dbadapter.table(extendTarget);
          expect(extendMetadata?.fields?.map((f) => f.name)).to.eql(["id", "name", "status"]);
          const extendRows = await selectAllLakehouseRows(extendTable);
          expect(extendRows.map(({ id, name }) => ({ id, name }))).to.eql([
            { id: 1, name: "alice" },
            { id: 2, name: "bob" },
          ]);

          // 3b. SYNCHRONIZE: remove column `legacy_col`
          writeDefinitionFile(
            projectDir,
            `${syncTable}.sqlx`,
            `config { type: "incremental", onSchemaChange: "SYNCHRONIZE", lakehouse: {} }
SELECT 1 AS id, 'alice' AS name, 'old' AS legacy_col`,
          );
          const syncRun1 = await runDataform(projectDir, [`--actions=${syncTable}`]);
          expect(
            syncRun1.exitCode,
            `stdout:\n${syncRun1.stdout}\nstderr:\n${syncRun1.stderr}`,
          ).equals(0);

          writeDefinitionFile(
            projectDir,
            `${syncTable}.sqlx`,
            `config { type: "incremental", onSchemaChange: "SYNCHRONIZE", lakehouse: {} }
SELECT 2 AS id, 'bob' AS name`,
          );
          // TODO: Once PR #2320 lands (dynamic DML inside the schema-change script), remove this
          // fallback and require `syncRun2.exitCode === 0` directly (and assert `status: "active"`
          // on row 2 in 3a above).
          const syncRun2 = await runDataform(projectDir, [`--actions=${syncTable}`]);
          if (syncRun2.exitCode !== 0) {
            const parsed2 = parseRunResultJson(syncRun2);
            const errorMessages = (parsed2.actions?.[0]?.tasks || [])
              .map((t: { errorMessage?: string }) => t.errorMessage || "")
              .join("\n")
              .toLowerCase();
            expect(errorMessages).to.include("legacy_col");
            const syncRun3 = await runDataform(projectDir, [`--actions=${syncTable}`]);
            expect(
              syncRun3.exitCode,
              `stdout:\n${syncRun3.stdout}\nstderr:\n${syncRun3.stderr}`,
            ).equals(0);
          }

          const syncMetadata = await dbadapter.table(syncTarget);
          expect(syncMetadata?.fields?.map((f) => f.name)).to.eql(["id", "name"]);
          expect(await selectAllLakehouseRows(syncTable)).to.eql([
            { id: 1, name: "alice" },
            { id: 2, name: "bob" },
          ]);

          const leftoverScratchTables = await dbadapter.tables(
            INTEGRATION_TEST_PROJECT,
            scratchDataset,
          );
          expect(leftoverScratchTables).to.eql([]);
        },
      );
    },
  );

  // Case 4: `operations` with `hasOutput: true`.
  test(
    "operations: creates a 4-part Lakehouse table when hasOutput is true",
    { timeout: 180000 },
    async () => {
      await withLakehouseProject(
        "df_e2e_lh_op",
        async ({ projectDir, registerTableForCleanup }) => {
          const opTable = makeUniqueTableName("op");
          registerTableForCleanup(opTable);

          writeDefinitionFile(
            projectDir,
            `${opTable}.sqlx`,
            `config {
  type: "operations",
  hasOutput: true,
  lakehouse: {}
}
CREATE OR REPLACE TABLE \${self()} AS
SELECT 1 AS id, 'from_operation' AS source`,
          );

          const res = await runDataform(projectDir);
          expect(res.exitCode, `stdout:\n${res.stdout}\nstderr:\n${res.stderr}`).equals(0);
          const parsed = parseRunResultJson(res);
          expectUnwrappedTask(parsed.actions?.[0]?.tasks?.[0]);
          expect(await selectAllLakehouseRows(opTable)).to.eql([
            { id: 1, source: "from_operation" },
          ]);
        },
      );
    },
  );

  // Case 5: Table and top-level column descriptions set after run (nested STRUCT ignored safely).
  test(
    "descriptions: sets table and top-level column descriptions while ignoring nested STRUCT fields",
    { timeout: 180000 },
    async () => {
      await withLakehouseProject(
        "df_e2e_lh_meta",
        async ({ projectDir, registerTableForCleanup }) => {
          const tableName = makeUniqueTableName("meta");
          const target = registerTableForCleanup(tableName);

          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config {
  type: "table",
  description: "Lakehouse E2E test table's description",
  columns: {
    id: "Primary identifier",
    details: {
      description: "Top-level struct column",
      columns: {
        nested_note: "Nested description skipped for Lakehouse"
      }
    }
  },
  lakehouse: {}
}
SELECT 1 AS id, STRUCT('ok' AS nested_note) AS details`,
          );

          const res = await runDataform(projectDir);
          expect(res.exitCode, `stdout:\n${res.stdout}\nstderr:\n${res.stderr}`).equals(0);

          const metadata = await dbadapter.table(target);
          expect(metadata?.description).to.equal("Lakehouse E2E test table's description");
          const idField = metadata?.fields?.find((f) => f.name === "id");
          const detailsField = metadata?.fields?.find((f) => f.name === "details");
          const nestedNoteField = detailsField?.struct?.fields?.find(
            (f) => f.name === "nested_note",
          );
          expect(idField?.description).to.equal("Primary identifier");
          expect(detailsField?.description).to.equal("Top-level struct column");
          expect(nestedNoteField).to.not.equal(undefined);
          expect(nestedNoteField?.description || "").to.equal("");
        },
      );
    },
  );

  // Case 6: Non-existent namespace fails with a clear error.
  test(
    "missing namespace: fails with a clear not-found error without attempting CREATE SCHEMA",
    { timeout: 120000 },
    async () => {
      await withLakehouseProject("df_e2e_lh_missing_ns", async ({ projectDir }) => {
        const tableName = makeUniqueTableName("bad_ns");
        const missingNamespace = `nonexistent_ns_${makeUniqueSuffix()}`;
        writeDefinitionFile(
          projectDir,
          `${tableName}.sqlx`,
          `config {
  type: "table",
  lakehouse: {
    catalog: "${INTEGRATION_TEST_LAKEHOUSE_CATALOG}",
    namespace: "${missingNamespace}"
  }
}
SELECT 1 AS id`,
        );

        // Dry run always emits the exact SQL sent to BigQuery in `compiledSql` (including any
        // `CREATE SCHEMA` wrapper if Runner tried to create the missing namespace). Because
        // BigQuery rejects the failed real run with 404 before assigning a jobId, inspect the
        // dry-run SQL directly to verify no `CREATE SCHEMA` statement was attempted.
        const dryRunRes = await runDataform(projectDir, ["--dry-run"]);
        expect(dryRunRes.exitCode).to.not.equal(0);
        const parsedDryRun = parseRunResultJson(dryRunRes);
        expect(parsedDryRun.status).equals(dataform.RunResult.ExecutionStatus.FAILED);
        const dryRunSql = parsedDryRun.actions?.[0]?.tasks?.[0]?.compiledSql || "";
        expect(dryRunSql).to.match(/create\s+or\s+replace\s+table/i);
        expect(dryRunSql).to.not.match(/create\s+schema/i);

        const res = await runDataform(projectDir);
        expect(res.exitCode).to.not.equal(0);
        const parsed = parseRunResultJson(res);
        expect(parsed.status).equals(dataform.RunResult.ExecutionStatus.FAILED);
        const task = parsed.actions?.[0]?.tasks?.[0];
        expectUnwrappedTask(task);
        const errorMessage = (task?.errorMessage || "").toLowerCase();
        expect(errorMessage).to.include("not found");
        expect(errorMessage).to.include(missingNamespace.toLowerCase());
      });
    },
  );

  // Case 7: Regular 3-part `view` reading a Lakehouse table through `ref()`.
  test(
    "ref(): regular 3-part view reads a 4-part Lakehouse table",
    { timeout: 180000 },
    async () => {
      await withLakehouseProject(
        "df_e2e_lh_view",
        async ({ projectDir, scratchDataset, registerTableForCleanup }) => {
          const lhTable = makeUniqueTableName("src");
          const viewName = `view_over_${lhTable}`;
          registerTableForCleanup(lhTable);

          writeDefinitionFile(
            projectDir,
            `${lhTable}.sqlx`,
            `config { type: "table", lakehouse: {} }
SELECT 1 AS id, 'lakehouse_row' AS label`,
          );
          writeDefinitionFile(
            projectDir,
            `${viewName}.sqlx`,
            `config { type: "view" }
SELECT id, UPPER(label) AS upper_label FROM \${ref("${lhTable}")}`,
          );

          const res = await runDataform(projectDir, [`--actions=${viewName}`, "--include-deps"]);
          expect(res.exitCode, `stdout:\n${res.stdout}\nstderr:\n${res.stderr}`).equals(0);

          const { rows } = await dbadapter.execute(
            `SELECT * FROM \`${INTEGRATION_TEST_PROJECT}.${scratchDataset}.${viewName}\``,
            { bigquery: { location: INTEGRATION_TEST_LAKEHOUSE_LOCATION } },
          );
          expect(rows).to.eql([{ id: 1, upper_label: "LAKEHOUSE_ROW" }]);
        },
      );
    },
  );

  // Case 8: Location resolution with and without `defaultLocation`.
  test(
    "location: resolves catalog location with defaultLocation and without defaultLocation",
    { timeout: 240000 },
    async () => {
      const conflictingLocation =
        INTEGRATION_TEST_LAKEHOUSE_LOCATION.toUpperCase() === "US" ? "EU" : "US";
      const conflictingCredsPath = writeCredentialsWithLocation(conflictingLocation);
      const noLocationCredsPath = writeCredentialsWithLocation(undefined);

      // 8a. With `defaultLocation`: overrides conflicting location in credentials.
      await withLakehouseProject(
        "df_e2e_lh_loc",
        async ({ projectDir, registerTableForCleanup }) => {
          const tableName = makeUniqueTableName("loc_explicit");
          const target = registerTableForCleanup(tableName);
          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config { type: "table", description: "Explicit location", lakehouse: {} }
SELECT 1 AS id`,
          );

          const res = await runDataform(projectDir, [], conflictingCredsPath);
          expect(res.exitCode, `stdout:\n${res.stdout}\nstderr:\n${res.stderr}`).equals(0);
          expect(await selectAllLakehouseRows(tableName)).to.eql([{ id: 1 }]);
          expect((await dbadapter.table(target))?.description).to.equal("Explicit location");
        },
        { defaultLocation: INTEGRATION_TEST_LAKEHOUSE_LOCATION },
      );

      // 8b. Without `defaultLocation`: fails when credentials force wrong location, succeeds when
      // credentials also omit location (BigQuery infers catalog location from 4-part target).
      await withLakehouseProject(
        "df_e2e_lh_loc",
        async ({ projectDir, registerTableForCleanup }) => {
          const tableName = makeUniqueTableName("loc_inferred");
          registerTableForCleanup(tableName);
          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config { type: "table", lakehouse: {} }
SELECT 1 AS id`,
          );

          const failedRes = await runDataform(projectDir, [], conflictingCredsPath);
          expect(failedRes.exitCode).to.not.equal(0);
          const parsedFailed = parseRunResultJson(failedRes);
          const failedError = (
            parsedFailed.actions?.[0]?.tasks?.[0]?.errorMessage || ""
          ).toLowerCase();
          expect(failedError).to.include("not found");
          expect(failedError).to.include(conflictingLocation.toLowerCase());

          const inferredRes = await runDataform(projectDir, [], noLocationCredsPath);
          expect(
            inferredRes.exitCode,
            `stdout:\n${inferredRes.stdout}\nstderr:\n${inferredRes.stderr}`,
          ).equals(0);
          expect(await selectAllLakehouseRows(tableName)).to.eql([{ id: 1 }]);
        },
        { defaultLocation: undefined },
      );
    },
  );

  // Case 9: `defaultDataset` missing vs already existing (no CREATE SCHEMA emitted).
  test(
    "defaultDataset: creates missing scratch dataset once and skips CREATE SCHEMA when it exists",
    { timeout: 240000 },
    async () => {
      // 9a. Missing `defaultDataset` -> Runner wraps first run with `create schema if not exists`
      // in the Lakehouse location, and subsequent incremental runs emit no `CREATE SCHEMA`.
      await withLakehouseProject(
        "df_e2e_lh_ds_missing",
        async ({ projectDir, scratchDataset, registerTableForCleanup }) => {
          const tableName = makeUniqueTableName("ds_missing");
          const target = registerTableForCleanup(tableName);
          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config { type: "incremental", onSchemaChange: "EXTEND", lakehouse: {} }
SELECT 1 AS id, 'a' AS name`,
          );

          const run1 = await runDataform(projectDir);
          expect(run1.exitCode, `stdout:\n${run1.stdout}\nstderr:\n${run1.stderr}`).equals(0);
          const parsed1 = parseRunResultJson(run1);
          // Runner sets `compiledSql` in non-dry runs only when it wraps the task's SQL
          // (here, prepending `CREATE SCHEMA IF NOT EXISTS` for the missing scratch dataset).
          expect(parsed1.actions?.[0]?.tasks?.[0]?.compiledSql?.toLowerCase() || "").to.include(
            `create schema if not exists \`${INTEGRATION_TEST_PROJECT}.${scratchDataset}\``.toLowerCase(),
          );
          await expectDatasetInLocation(scratchDataset, INTEGRATION_TEST_LAKEHOUSE_LOCATION);

          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config { type: "incremental", onSchemaChange: "EXTEND", lakehouse: {} }
SELECT 2 AS id, 'b' AS name, 'active' AS status`,
          );
          const run2 = await runDataform(projectDir);
          expect(run2.exitCode, `stdout:\n${run2.stdout}\nstderr:\n${run2.stderr}`).equals(0);
          const parsed2 = parseRunResultJson(run2);
          expectUnwrappedTask(parsed2.actions?.[0]?.tasks?.[0]);
          const run2Sql = await fetchExecutedJobQuery(
            parsed2.actions?.[0]?.tasks?.[0]?.metadata?.bigquery?.jobId,
          );
          expect(run2Sql).to.not.match(/create\s+schema/i);

          const metadata = await dbadapter.table(target);
          expect(metadata?.fields?.map((f) => f.name)).to.eql(["id", "name", "status"]);
        },
      );

      // 9b. Existing `defaultDataset` -> neither Runner nor ExecutionSql emits `CREATE SCHEMA`.
      await withLakehouseProject(
        "df_e2e_lh_ds_existing",
        async ({ projectDir, scratchDataset, registerTableForCleanup }) => {
          const tableName = makeUniqueTableName("ds_existing");
          const target = registerTableForCleanup(tableName);

          await dbadapter.execute(
            `CREATE SCHEMA IF NOT EXISTS \`${INTEGRATION_TEST_PROJECT}.${scratchDataset}\` ` +
              `OPTIONS(location = '${INTEGRATION_TEST_LAKEHOUSE_LOCATION}')`,
            { bigquery: { location: INTEGRATION_TEST_LAKEHOUSE_LOCATION } },
          );

          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config { type: "incremental", onSchemaChange: "EXTEND", lakehouse: {} }
SELECT 1 AS id, 'alice' AS name`,
          );
          const run1 = await runDataform(projectDir);
          expect(run1.exitCode, `stdout:\n${run1.stdout}\nstderr:\n${run1.stderr}`).equals(0);
          const parsed1 = parseRunResultJson(run1);
          expectUnwrappedTask(parsed1.actions?.[0]?.tasks?.[0]);
          const run1Sql = await fetchExecutedJobQuery(
            parsed1.actions?.[0]?.tasks?.[0]?.metadata?.bigquery?.jobId,
          );
          expect(run1Sql).to.not.match(/create\s+schema/i);

          writeDefinitionFile(
            projectDir,
            `${tableName}.sqlx`,
            `config { type: "incremental", onSchemaChange: "EXTEND", lakehouse: {} }
SELECT 2 AS id, 'bob' AS name, 'active' AS status`,
          );
          const run2 = await runDataform(projectDir);
          expect(run2.exitCode, `stdout:\n${run2.stdout}\nstderr:\n${run2.stderr}`).equals(0);
          const parsed2 = parseRunResultJson(run2);
          expectUnwrappedTask(parsed2.actions?.[0]?.tasks?.[0]);
          const run2Sql = await fetchExecutedJobQuery(
            parsed2.actions?.[0]?.tasks?.[0]?.metadata?.bigquery?.jobId,
          );
          expect(run2Sql).to.not.match(/create\s+schema/i);

          const metadata = await dbadapter.table(target);
          expect(metadata?.fields?.map((f) => f.name)).to.eql(["id", "name", "status"]);
        },
      );
    },
  );
});
