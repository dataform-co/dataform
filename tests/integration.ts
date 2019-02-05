import { expect } from "chai";
import * as childProcess from "child_process";
import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { query } from "@dataform/api";
import * as protos from "@dataform/protos";
import { asPlainObject } from "./utils";
import { create } from "../core/adapters";

interface ITableInfo {
  schema: string;
  table: string;
  type: string;
}

interface ITestConfig {
  warehouse: string;
  profile: protos.IProfile;
  projectDir: string;
  projectConf: protos.IProjectConfig;
  defaultSchema: string;
  assertionSchema: string;
  resultPath: string;
  command: string;
  workingDir: string;
}

interface IExpectedResult {
  id: string;
  data: object[];
}

function queryRun(sqlQuery: string, testConfig: ITestConfig) {
  return query.run(protos.Profile.create(testConfig.profile), sqlQuery, path.resolve(testConfig.projectDir));
}

function getTarget(schema: string, table: string, testConfig: ITestConfig) {
  const adapter = create({ ...testConfig.projectConf, gcloudProjectId: null });
  const target = adapter.resolveTarget({ schema: schema, name: table });

  return target.replace(/`/g, "\\`");
}

function deleteTables(tables: ITableInfo[], testConfig: ITestConfig) {
  return Promise.all(
    tables.map(item => {
      const target = getTarget(item.schema, item.table, testConfig);
      const sqlDelete = `drop ${item.type} if exists ${target}`;

      return queryRun(sqlDelete, testConfig);
    })
  );
}

function getTables(schema: string, testConfig: ITestConfig) {
  const tablesQuery = {
    bigquery: `
      select
        table_id as table,
        dataset_id as schema,
        case type when 2 then 'view' else 'table' end as type
      from ${schema}.__TABLES__`,
    redshift: `
      select
        table_name as table,
        table_schema as schema,
        case table_type when 'VIEW' then 'view' else 'table' end as type
      from information_schema.tables
      where table_schema != 'information_schema'
        and table_schema != 'pg_catalog'
        and table_schema != 'pg_internal'
        and table_schema = '${schema}'`,
    snowflake: `
      select
        table_name as "table",
        table_schema as "schema",
        case table_type when 'VIEW' then 'view' else 'table' end as "type"
      from information_schema.tables
      where LOWER(table_schema) != 'information_schema'
        and LOWER(table_schema) != 'pg_catalog'
        and LOWER(table_schema) != 'pg_internal'
        and LOWER(table_schema) = '${schema}'`
  };

  return queryRun(tablesQuery[testConfig.warehouse], testConfig);
}

function getData(expectedResult: IExpectedResult[], schema: string, testConfig: ITestConfig) {
  return Promise.all(
    expectedResult.map(item => {
      const target = getTarget(schema, item.id, testConfig);
      const sqlSelect = `select * from ${target}`;

      return queryRun(sqlSelect, testConfig);
    })
  );
}

function getTestConfig(warehouse: string): ITestConfig {
  const profilePath = path.resolve(`../../test_profiles/${warehouse}.json`);
  const profile = fs.existsSync(profilePath) ? JSON.parse(fs.readFileSync(profilePath, "utf8")) : null;
  const projectDir = path.resolve(`../examples/${warehouse}`);
  const projectConf = JSON.parse(fs.readFileSync(path.resolve(projectDir, "./dataform.json"), "utf8"));
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "df-"));
  const resultPath = path.resolve(tempDir, "./executed_graph.json");
  const command = `./scripts/run run examples/${warehouse} --profile="${profilePath}" --result-path="${resultPath}"`;

  return {
    warehouse,
    profile,
    projectDir,
    projectConf,
    defaultSchema: projectConf.defaultSchema,
    assertionSchema: projectConf.assertionSchema,
    resultPath,
    command,
    workingDir: "../../"
  };
}

function getTestRunCommand(testConfig: ITestConfig, expectedResult: IExpectedResult[], incrementalLength: number) {
  return async () => {
    // run the command
    await expect(async () => {
      childProcess.execSync(testConfig.command, { cwd: testConfig.workingDir });
    }).to.not.throw();

    // check for errors in graph
    const buf = fs.readFileSync(testConfig.resultPath);
    const graph = JSON.parse(buf.toString());

    expect(graph).to.have.property("ok").that.to.be.true;
    expect(graph)
      .to.have.property("nodes")
      .to.be.an("array").that.is.not.empty;

    // check for errors in database
    const data = await getData(expectedResult, testConfig.defaultSchema, testConfig);
    expectedResult.forEach((item, i) => {
      const isIncremental = item.id === "example_incremental";
      const dataLength = isIncremental ? incrementalLength: item.data.length;

      expect(data[i])
        .to.be.an("array")
        .that.have.lengthOf(dataLength);

      if (!isIncremental) {
        expect(asPlainObject(data[i])).to.have.deep.members(asPlainObject(item.data));
      }
    });
  };
}

function getHookBefore(testConfig: ITestConfig) {
  return async () => {
    // get all tables in dataset
    const dTables = await getTables(testConfig.defaultSchema, testConfig);
    const aTables = await getTables(testConfig.assertionSchema, testConfig);

    // delete existing tables
    if (aTables.length > 0) {
      await deleteTables(aTables, testConfig);
    }
    if (dTables.length > 0) {
      await deleteTables(dTables, testConfig);
    }
  };
}

describe("@dataform/integration", () => {
  describe("run", () => {
    describe("bigquery", function() {
      this.timeout(300000);

      const testConfig = getTestConfig("bigquery");
      const expectedResult = [
        { id: "example_backticks", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "example_table", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "example_js_blocks", data: [{ foo: 1 }] },
        { id: "example_deferred", data: [{ test: 1 }] },
        { id: "example_view", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "sample_data", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "example_incremental", data: [] }
      ];

      // check project credentials
      this.pending = !testConfig.profile;
      if (this.isPending()) {
        console.log("No Bigquery profile config, tests will be skipped!");
      }

      before("clear_schema", getHookBefore(testConfig));

      it("bigquery_1", getTestRunCommand(testConfig, expectedResult, 1));

      it("bigquery_2", getTestRunCommand(testConfig, expectedResult, 2));
    });

    describe("redshift", function() {
      this.timeout(300000);

      const testConfig = getTestConfig("redshift");
      const expectedResult = [
        { id: "example_table", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "example_table_dependency", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "example_view", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "sample_data", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "example_incremental", data: [] }
      ];

      // check project credentials
      this.pending = !testConfig.profile;
      if (this.isPending()) {
        console.log("No Redshift profile config, tests will be skipped!");
      }

      before("clear_schema", getHookBefore(testConfig));

      it("redshift_1", getTestRunCommand(testConfig, expectedResult, 1));

      it("redshift_2", getTestRunCommand(testConfig, expectedResult, 2));
    });

    describe("snowflake", function() {
      this.timeout(300000);

      const testConfig = getTestConfig("snowflake");
      const expectedResult = [
        { id: "example_table", data: [{ SAMPLE_COLUMN: 1 }, { SAMPLE_COLUMN: 2 }, { SAMPLE_COLUMN: 3 }] },
        { id: "example_view", data: [{ SAMPLE_COLUMN: 1 }, { SAMPLE_COLUMN: 2 }, { SAMPLE_COLUMN: 3 }] },
        { id: "sample_data", data: [{ SAMPLE_COLUMN: 1 }, { SAMPLE_COLUMN: 2 }, { SAMPLE_COLUMN: 3 }] },
        { id: "example_incremental", data: [] }
      ];

      // check project credentials
      this.pending = !testConfig.profile;
      if (this.isPending()) {
        console.log("No Snowflake profile config, tests will be skipped!");
      }

      before("clear_schema", getHookBefore(testConfig));

      it("snowflake_1", getTestRunCommand(testConfig, expectedResult, 1));

      it("snowflake_2", getTestRunCommand(testConfig, expectedResult, 2));
    });
  });
});
