import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import * as dfapi from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import {
  alterWorkflowSettings,
  CREDENTIALS_PATH,
  INTEGRATION_TEST_PROJECT,
  runCli,
  setupProject,
} from "df/cli/index_test_base";
import { test, writeDefinitionFile } from "df/testing";
import { dropDatasets, dropStaleDatasets, makeUniqueSuffix } from "df/testing/bigquery";
import { integrationSuite, readTestCredentialsConfig } from "df/testing/credentials";
import { TmpDirFixture } from "df/testing/fixtures";

const LOCATION_RESOLUTION_DATASET = "dataform_e2e_loc";

integrationSuite("location resolution", ({ before, beforeEach, afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

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

  afterEach("drop datasets", async () => {
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

  function writeCredentialsWithLocation(location: string | undefined): string {
    const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), "credentials.json");
    fs.writeFileSync(
      credentialsPath,
      JSON.stringify({ ...readTestCredentialsConfig(CREDENTIALS_PATH), location }),
    );
    return credentialsPath;
  }

  async function runProject(projectDir: string, credentialsPath: string, args: string[] = []) {
    const runResult = await runCli("run", [projectDir, "--credentials", credentialsPath, ...args]);
    expect(runResult.exitCode, `stdout:\n${runResult.stdout}\nstderr:\n${runResult.stderr}`).equals(
      0,
    );
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

  async function expectDatasetMissing(dataset: string) {
    for (const location of ALL_LOCATIONS) {
      const { rows } = await dbadapter.execute(
        `select location
         from \`${INTEGRATION_TEST_PROJECT}\`.\`region-${location.toLowerCase()}\`.INFORMATION_SCHEMA.SCHEMATA
         where schema_name = '${dataset}'`,
        { bigquery: { location } },
      );
      expect(rows.length, `dataset ${dataset} unexpectedly exists in ${location}`).equals(0);
    }
  }

  test(
    "without any location, new datasets are created in the location of the data read",
    { timeout: 240000 },
    async () => {
      // Source data in EXPECTED_LOCATION.
      const { projectDir: sourceProjectDir, dataset: sourceDataset } =
        await setupLocationProject(EXPECTED_LOCATION);
      await runProject(sourceProjectDir, writeCredentialsWithLocation(EXPECTED_LOCATION));
      const source = `\`${INTEGRATION_TEST_PROJECT}.${sourceDataset}.example\``;

      // A project without a location anywhere, writing to new datasets.
      const { projectDir, dataset } = await setupLocationProject(undefined);
      const assertionDataset = `${LOCATION_RESOLUTION_DATASET}_${makeUniqueSuffix()}`;
      datasetsToDrop.push(assertionDataset);
      alterWorkflowSettings(projectDir, { defaultAssertionDataset: assertionDataset });
      writeDefinitionFile(
        projectDir,
        "example.sqlx",
        `
config { type: "table", assertions: { nonNull: ["id"] } }
SELECT * FROM ${source}
`,
      );
      // DECLARE must stay at the start of the block the statement is wrapped in.
      writeDefinitionFile(
        projectDir,
        "example_incremental.sqlx",
        `
config { type: "incremental" }
pre_operations {
  DECLARE min_id INT64 DEFAULT 0;
}
SELECT * FROM ${source} WHERE id >= min_id
`,
      );
      writeDefinitionFile(
        projectDir,
        "example_operation.sqlx",
        `
config { type: "operations", hasOutput: true }
CREATE OR REPLACE TABLE \${self()} AS SELECT * FROM ${source}
`,
      );

      await runProject(projectDir, writeCredentialsWithLocation(undefined));

      await expectDatasetLocation(dataset, EXPECTED_LOCATION);
      await expectDatasetLocation(assertionDataset, EXPECTED_LOCATION);
    },
  );

  test("without any location, dry runs don't create datasets", { timeout: 120000 }, async () => {
    const { projectDir, dataset } = await setupLocationProject(undefined);

    await runProject(projectDir, writeCredentialsWithLocation(undefined), ["--dry-run"]);

    await expectDatasetMissing(dataset);
  });

  test(
    "dry runs fully check the query of tables writing to a dataset that doesn't exist yet",
    { timeout: 120000 },
    async () => {
      const { projectDir, dataset } = await setupLocationProject(EXPECTED_LOCATION);
      writeDefinitionFile(
        projectDir,
        "example.sqlx",
        `
config { type: "table" }
pre_operations {
  DECLARE min_id INT64 DEFAULT 0;
}
SELECT no_such_column FROM (SELECT 1 AS id) WHERE id >= min_id
post_operations {
  SET min_id = 1;
}
`,
      );

      const runResult = await runCli("run", [
        projectDir,
        "--credentials",
        writeCredentialsWithLocation(EXPECTED_LOCATION),
        "--dry-run",
      ]);

      // Only a syntax check of the script creating the dataset wouldn't catch this.
      expect(runResult.exitCode).equals(1);
      const output = `${runResult.stdout}\n${runResult.stderr}`;
      expect(output).to.include("Unrecognized name: no_such_column");

      // The error position refers to the printed SQL (the statement wrapped in the script creating
      // the dataset), even though the error comes from dry-running the query separately.
      const printedSqlLines = output
        .split("\n")
        .filter((line) => line.trimStart().startsWith("> "))
        .map((line) => line.trimStart().slice(2));
      const [, line, column] = output.match(/no_such_column at \[(\d+):(\d+)\]/);
      expect(printedSqlLines[Number(line) - 1].slice(Number(column) - 1)).to.match(
        /^no_such_column/,
        output,
      );
      await expectDatasetMissing(dataset);
    },
  );
});
