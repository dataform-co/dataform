import { expect } from "chai";
import * as childProcess from "child_process";
import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { query } from "@dataform/api";
import * as protos from "@dataform/protos";
import { asPlainObject } from "./utils";

interface ITableInfo {
  dataset_id: string;
  table_id: string;
  type: number;
}

function queryRun(sqlQuery: string, profile: protos.IProfile, projectDir: string) {
  return query.run(protos.Profile.create(profile), sqlQuery, path.resolve(projectDir)).catch(e => e);
}

function deleteTables(tables: ITableInfo[], profile: protos.IProfile, projectDir: string) {
  return Promise.all(
    tables.map(item => {
      const type = item.type === 1 ? "table" : "view";
      const sqlDelete = `drop ${type} if exists ${item.dataset_id}.${item.table_id}`;

      return queryRun(sqlDelete, profile, projectDir);
    })
  );
}

function getTables(datasetId: string, profile: protos.IProfile, projectDir: string) {
  const sqlTables = `select * from ${datasetId}.__TABLES__`;

  return queryRun(sqlTables, profile, projectDir);
}

function getData(
  expectedResult: { [x: string]: any },
  defaultSchema: string,
  profile: protos.IProfile,
  projectDir: string
) {
  return Promise.all(
    expectedResult.map(item => {
      const sqlSelect = `select * from ${defaultSchema}.${item.id}`;

      return queryRun(sqlSelect, profile, projectDir);
    })
  );
}

describe("@dataform/integration", () => {
  describe("run_bigquery", function() {
    this.timeout(300000);
    const warehouse = "bigquery";
    const profilePath = path.resolve(`../../test_profiles/${warehouse}.json`);
    const profile = JSON.parse(fs.readFileSync(profilePath, "utf8"));
    const projectDir = path.resolve(`../examples/${warehouse}`);
    const projectConf = JSON.parse(fs.readFileSync(path.resolve(projectDir, "./dataform.json"), "utf8"));

    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "df-"));
    const resultPath = path.resolve(tempDir, "./executed_graph.json");
    const command = `./scripts/run run examples/${warehouse} --profile="${profilePath}" --result-path="${resultPath}"`;

    const expectedResult = [
      { id: "example_backticks", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_table", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "example_js_blocks", data: [{ foo: 1 }] },
      { id: "example_deferred", data: [{ test: 1 }] },
      { id: "example_view", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] },
      { id: "sample_data", data: [{ sample: 1 }, { sample: 2 }, { sample: 3 }] }
    ];

    before("clear_dataset", async () => {
      // get all tables in dataset
      const dTables = await getTables(projectConf.defaultSchema, profile, projectDir);
      const aTables = await getTables(projectConf.assertionSchema, profile, projectDir);

      // delete existing tables
      if (dTables.length > 0) {
        await deleteTables(dTables, profile, projectDir);
      }
      if (aTables.length > 0) {
        await deleteTables(aTables, profile, projectDir);
      }
    });

    const bigqueryTest = async () => {
      // run the command
      await expect(async () => {
        childProcess.execSync(command, { cwd: "../../" });
      }).to.not.throw();

      // check for errors in graph
      const buf = fs.readFileSync(resultPath);
      const graph = JSON.parse(buf.toString());

      expect(graph).to.have.property("ok").that.to.be.true;
      expect(graph)
        .to.have.property("nodes")
        .to.be.an("array").that.is.not.empty;

      // check for errors in database
      const data = await getData(expectedResult, projectConf.defaultSchema, profile, projectDir);

      expectedResult.forEach((item, i) => {
        expect(data[i])
          .to.be.an("array")
          .that.have.lengthOf(item.data.length);
        expect(asPlainObject(data[i])).to.have.deep.members(asPlainObject(item.data));
      });

      const sqlIncremental = `select * from ${projectConf.defaultSchema}.example_incremental`;
      const incremental = await queryRun(sqlIncremental, profile, projectDir);

      expect(incremental).to.be.an("array").that.is.not.empty;
      expect(incremental[0])
        .to.have.property("ts")
        .that.to.have.property("value");

      expect(incremental[0].ts.value).to.satisfy(str => {
        const reTimestamp = /([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))T(([0-1]\d|2[0-3])(:([0-5]\d)){2}\.(\d{3}))Z/;
        return reTimestamp.test(str);
      });
    };

    it("bigquery_1", bigqueryTest);

    it("bigquery_2", bigqueryTest);
  });
});
