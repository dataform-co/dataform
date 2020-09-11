import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { compile, keyBy } from "df/tests/integration/utils";
import { PrestoFixture, prestoTestCredentials } from "df/tools/presto/presto_fixture";

suite("@dataform/integration/presto", { parallel: true }, ({ before, after }) => {
  const _ = new PrestoFixture(1234, before, after);

  let dbadapter: dbadapters.IDbAdapter;

  before("create adapter", async () => {
    dbadapter = await dbadapters.create(prestoTestCredentials, "presto", {
      disableSslForTestsOnly: true
    });
  });

  after("close adapter", async () => dbadapter.close());

  test("run", { timeout: 60000 }, async () => {
    const compiledGraph = await compile("tests/integration/presto_project", "project_e2e");

    // Run the project.
    const executionGraph = await dfapi.build(compiledGraph, {}, dbadapter);
    const executedGraph = await dfapi.run(dbadapter, executionGraph).result();

    const actionMap = keyBy(executedGraph.actions, v => v.name);
    expect(Object.keys(actionMap).length).eql(1);

    for (const actionName of Object.keys(actionMap)) {
      expect(actionMap[actionName].status).equals(dataform.ActionResult.ExecutionStatus.SUCCESSFUL);
    }
  });

  test("catalog inspection", { timeout: 60000 }, async () => {
    const schemas = await dbadapter.schemas("");
    // Some of the schemas defined in the docker image.
    [
      "system.information_schema",
      "tpch.information_schema",
      "memory.default",
      "jmx.current",
      "tpcds.information_schema"
    ].forEach(schema => {
      expect(schemas).to.include(schema);
    });
    // Some of the tables defined in the docker image.
    // TODO: Fix equivalence testing.
    // const tables = await dbadapter.tables();
    // [
    //   { database: "tpcds", schema: "sf1000", table: "call_center" },
    //   {
    //     database: "jmx",
    //     schema: "current",
    //     table: "com.sun.management:type=diagnosticcommand"
    //   }
    // ].forEach(target => {
    //   expect(tables).to.include(dataform.Target.create(target));
    // });
  });
});
