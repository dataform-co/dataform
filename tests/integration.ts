import { expect } from "chai";
import * as childProcess from "child_process";
import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { query } from "@dataform/api";
import * as protos from "@dataform/protos";
import { asPlainObject } from "./utils";

interface ITableInfo {
  dataset: string;
  table: string;
  type: number | string;
}

interface ITestConfig {
  warehouse: string;
  profile: protos.IProfile;
  projectDir: string;
  defaultSchema: string;
  assertionSchema: string;
  resultPath: string;
  command: string;
  workingDir: string;
}

function queryRun(sqlQuery: string, testConfig: ITestConfig) {
  return query
    .run(protos.Profile.create(testConfig.profile), sqlQuery, path.resolve(testConfig.projectDir))
    .catch(e => e);
}

function deleteTables(tables: ITableInfo[], testConfig: ITestConfig) {
  return Promise.all(
    tables.map(item => {
      const sqlDelete = `drop ${item.type} if exists ${item.dataset}.${item.table}`;

      return queryRun(sqlDelete, testConfig);
    })
  );
}

function getTables(datasetId: string, testConfig: ITestConfig) {
  const tablesQuery = {
    bigquery: `
      select
        table_id as table,
        dataset_id as dataset,
        case type when 2 then 'view' else 'table' end as type
      from ${datasetId}.__TABLES__`,
    redshift: `
      select
        table_name as table,
        table_schema as dataset,
        case table_type when 'VIEW' then 'view' else 'table' end as type
      from information_schema.tables
      where table_schema != 'information_schema'
        and table_schema != 'pg_catalog'
        and table_schema != 'pg_internal'
        and table_schema = '${datasetId}'`
  };

  return queryRun(tablesQuery[testConfig.warehouse], testConfig);
}

function getData(expectedResult: { [x: string]: any }, datasetId: string, testConfig: ITestConfig) {
  return Promise.all(
    expectedResult.map(item => {
      const sqlSelect = `select * from ${datasetId}.${item.id}`;

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
    defaultSchema: projectConf.defaultSchema,
    assertionSchema: projectConf.assertionSchema,
    resultPath,
    command,
    workingDir: "../../"
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
        { id: "sample_data", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] }
      ];

      // check project credentials
      this.pending = !testConfig.profile;
      if (this.isPending()) {
        console.log("No Bigquery profile config, the tests will be skipped!");
      }

      before("clear_dataset", async () => {
        // get all tables in dataset
        const dTables = await getTables(testConfig.defaultSchema, testConfig);
        const aTables = await getTables(testConfig.assertionSchema, testConfig);

        // delete existing tables
        if (dTables.length > 0) {
          await deleteTables(dTables, testConfig);
        }
        if (aTables.length > 0) {
          await deleteTables(aTables, testConfig);
        }
      });

      const testRunCommand = async () => {
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
          expect(data[i])
            .to.be.an("array")
            .that.have.lengthOf(item.data.length);
          expect(asPlainObject(data[i])).to.have.deep.members(asPlainObject(item.data));
        });

        // check for errors in database (table with timestamp)
        const sqlIncremental = `select * from ${testConfig.defaultSchema}.example_incremental`;
        const incremental = await queryRun(sqlIncremental, testConfig);

        expect(incremental).to.be.an("array").that.is.not.empty;
        expect(incremental[0])
          .to.have.property("ts")
          .that.to.have.property("value");

        expect(incremental[0].ts.value).to.satisfy(str => {
          const reTimestamp = /([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))T(([0-1]\d|2[0-3])(:([0-5]\d)){2}\.(\d{3}))Z/;
          return reTimestamp.test(str);
        });
      };

      it("bigquery_1", testRunCommand);

      it("bigquery_2", testRunCommand);
    });

    describe("redshift", function() {
      this.timeout(300000);

      const testConfig = getTestConfig("redshift");
      const expectedResult = [
        { id: "example_table", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "example_table_dependency", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "example_view", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
        { id: "sample_data", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] }
      ];

      // check project credentials
      this.pending = !testConfig.profile;
      if (this.isPending()) {
        console.log("No Redshift profile config, the tests will be skipped!");
      }

      before("clear_dataset", async () => {
        // get all tables in dataset
        const dTables = await getTables(testConfig.defaultSchema, testConfig);
        const aTables = await getTables(testConfig.assertionSchema, testConfig);

        // delete existing tables
        if (dTables.length > 0) {
          await deleteTables(dTables, testConfig);
        }
        if (aTables.length > 0) {
          await deleteTables(aTables, testConfig);
        }
      });

      const testRunCommand = async () => {
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
          expect(data[i])
            .to.be.an("array")
            .that.have.lengthOf(item.data.length);
          expect(asPlainObject(data[i])).to.have.deep.members(asPlainObject(item.data));
        });

        // check for errors in database (table with timestamp)
        const sqlIncremental = `select * from ${testConfig.defaultSchema}.example_incremental`;
        const incremental = await queryRun(sqlIncremental, testConfig);

        expect(incremental).to.be.an("array").that.is.not.empty;
        expect(incremental[0])
          .to.have.property("ts")
          .that.to.be.an.instanceof(Date);
      };

      it("redshift_1", testRunCommand);

      it("redshift_2", testRunCommand);
    });
  });
});
