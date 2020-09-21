import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { compile, keyBy } from "df/tests/integration/utils";
import { PrestoFixture } from "df/tools/presto/presto_fixture";

suite("@dataform/integration/presto", { parallel: true }, ({ before, after }) => {
  const _ = new PrestoFixture(before, after);

  let dbadapter: dbadapters.IDbAdapter;

  before("create adapter", async () => {
    dbadapter = await dbadapters.create(PrestoFixture.PRESTO_TEST_CREDENTIALS, "presto");
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

  test("catalog inspection", async () => {
    const targets = await dbadapter.tables();
    const resolvedTargets = targets.map(
      target => `${target.database}.${target.schema}.${target.name}`
    );
    [
      "system.metadata.analyze_properties",
      "jmx.information_schema.applicable_roles",
      "system.information_schema.applicable_roles",
      "jmx.current.com.sun.management:type=diagnosticcommand",
      "memory.information_schema.applicable_roles"
    ].forEach(resolvedTarget => {
      expect(resolvedTargets).to.include(resolvedTarget);
    });
  });

  test("evaluation failure", async () => {
    const failedEvaluation = await dbadapter.evaluate("select\nz\nas\nx");
    expect(failedEvaluation.length).to.equal(1);
    expect(failedEvaluation[0].status).to.equal(
      dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE
    );
    expect(failedEvaluation[0].error.errorLocation.line).to.equal(2);
    expect(true).to.equal(false);
  });
});
