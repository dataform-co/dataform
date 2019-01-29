import { expect, assert } from "chai";
import * as childProcess from "child_process";
import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import { query, table } from "@dataform/api";
import * as protos from "@dataform/protos";

async function queryRun(sqlQuery: string, profile: string, projectDir: string) {
  return await query.run(
    protos.Profile.create(JSON.parse(fs.readFileSync(profile, "utf8"))),
    sqlQuery,
    path.resolve(projectDir)
  );
}

async function clearDataset(warehouse: string) {
  const profile = path.resolve(`../../test_profiles/${warehouse}.json`);
  const projectDir = path.resolve(`../examples/${warehouse}`);
  const projectConf = JSON.parse(fs.readFileSync(path.resolve(projectDir, "./dataform.json"), "utf8"));
  const datasetId = projectConf.defaultSchema;

  const sqlGetTables = `select * from ${datasetId}.__TABLES__`;

  const tables = await queryRun(sqlGetTables, profile, projectDir);
  // console.log("---- tables:", JSON.stringify(tables, null, 4));

  //TODO: need bulk delete query or delete the dataset
  const sqlDeleteTables = tables.map(item => `drop table if exists ${datasetId}.${item.table_id}`).join("\n;\n");
  console.log("---- sqlDelete:", sqlDeleteTables);

  // const result = await queryRun(sqlDeleteTables, profile, projectDir);
  // console.log("---- result:", result);



  // await table
  //   .get(protos.Profile.create(JSON.parse(fs.readFileSync(profile, "utf8"))), {
  //     schema: "dataform_example",
  //     name: "sample_data"
  //   })
  //   .then(schema => console.log("---- table.get:", JSON.stringify(schema, null, 4)));

  // await table
  //   .list(protos.Profile.create(JSON.parse(fs.readFileSync(profile, "utf8"))))
  //   .then(tables => console.log("---- table.list:", tables));

  // await query.compile(sqlQuery, projectDir)
  //   .then(compiledQuery => {
  //     console.log('---- compiledQuery: ', compiledQuery);
  //
  //     return query.run(
  //       protos.Profile.create(JSON.parse(fs.readFileSync(profile, "utf8"))),
  //       compiledQuery,
  //       path.resolve(projectDir)
  //     );
  //     }
  //   )
  //   .then(results => console.log("---- before:", JSON.stringify(results, null, 4)));

}

describe("@dataform/integration", () => {
  describe("run_bigquery", function() {
    this.timeout(60000);

    before('clear_dataset', async () => {
      // clear existing tables
      await clearDataset("bigquery");
    });

    it("bigquery", () => {
      expect(async () => {

        const profile = path.resolve('../../test_profiles/bigquery.json');
        const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "df-"));
        const resultPath = path.resolve(tempDir, "./executed_graph.json");
        const command = `./scripts/run run examples/bigquery --profile="${profile}" --result-path="${resultPath}"`;

        // console.log('---- profile: ', profile);
        // console.log('---- tempDir: ', tempDir);
        // console.log('---- resultPath: ', resultPath);
        // console.log('---- command: ', command);

        childProcess.execSync(command, { cwd: '../../' });

        const buf = fs.readFileSync(resultPath);

        // console.log('---- buf: ', buf);
        // console.log('---- result: ', buf.toString());

        const graph = JSON.parse(buf.toString());
        console.log('---- graph: ', graph);

      }).to.not.throw();
    });
  });
});