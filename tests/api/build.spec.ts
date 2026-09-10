import { assert, config, expect } from "chai";

import { Builder } from "df/cli/api";
import { equals } from "df/common/protos";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TEST_GRAPH } from "df/tests/api/utils";

config.truncateThreshold = 0;

suite("@dataform/api/build", () => {
  const TEST_STATE = dataform.WarehouseState.create({ tables: [] });
  test("exclude_disabled", () => {
    const builder = new Builder(TEST_GRAPH, { includeDependencies: true }, TEST_STATE);
    const executionGraph = builder.build();

    const actionA = executionGraph.actions.find(
      n => targetAsReadableString(n.target) === "schema.a"
    );
    const actionB = executionGraph.actions.find(
      n => targetAsReadableString(n.target) === "schema.b"
    );
    const actionC = executionGraph.actions.find(
      n => targetAsReadableString(n.target) === "schema.c"
    );

    assert.exists(actionA);
    assert.exists(actionB);
    assert.exists(actionC);

    expect(actionA.tasks.length).greaterThan(0);
    expect(actionB.tasks).deep.equal([]);
    expect(actionC.tasks.length).greaterThan(0);
  });

  test("build_with_errors", () => {
    expect(() => {
      const graphWithErrors: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery" },
        graphErrors: { compilationErrors: [{ message: "Some critical error" }] },
        tables: [{ target: { schema: "schema", name: "a" } }]
      });

      const builder = new Builder(graphWithErrors, {}, TEST_STATE);
      builder.build();
    }).to.throw();
  });

  test("action_types", () => {
    const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
      projectConfig: { warehouse: "bigquery" },
      tables: [
        { target: { schema: "schema", name: "a" }, type: "table" },
        {
          target: { schema: "schema", name: "b" },
          type: "incremental",
          where: "test"
        },
        { target: { schema: "schema", name: "c" }, type: "view" }
      ],
      operations: [
        {
          target: { schema: "schema", name: "d" },
          queries: ["create or replace view schema.someview as select 1 as test"]
        }
      ],
      assertions: [{ target: { schema: "schema", name: "e" } }]
    });

    const builder = new Builder(graph, {}, TEST_STATE);
    const executedGraph = builder.build();

    expect(executedGraph.actions.length).greaterThan(0);

    graph.tables.forEach((t: dataform.ITable) => {
      const action = executedGraph.actions.find(item =>
        equals(dataform.Target, item.target, t.target)
      );
      expect(action).to.include({ type: "table", target: t.target, tableType: t.type });
    });

    graph.operations.forEach((o: dataform.IOperation) => {
      const action = executedGraph.actions.find(item =>
        equals(dataform.Target, item.target, o.target)
      );
      expect(action).to.include({ type: "operation", target: o.target });
    });

    graph.assertions.forEach((a: dataform.IAssertion) => {
      const action = executedGraph.actions.find(item =>
        equals(dataform.Target, item.target, a.target)
      );
      expect(action).to.include({ type: "assertion" });
    });
  });

  test("table_enum_types", () => {
    const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
      projectConfig: { warehouse: "bigquery" },
      tables: [
        { target: { schema: "schema", name: "a" }, enumType: dataform.TableType.TABLE },
        {
          target: { schema: "schema", name: "b" },
          enumType: dataform.TableType.INCREMENTAL,
          where: "test"
        },
        { target: { schema: "schema", name: "c" }, enumType: dataform.TableType.VIEW }
      ]
    });

    const builder = new Builder(graph, {}, TEST_STATE);
    const executedGraph = builder.build();

    expect(executedGraph.actions.length).greaterThan(0);

    graph.tables.forEach((t: dataform.ITable) => {
      const action = executedGraph.actions.find(item =>
        equals(dataform.Target, item.target, t.target)
      );
      expect(action).to.include({
        type: "table",
        target: t.target,
        tableType: dataform.TableType[t.enumType].toLowerCase()
      });
    });
  });

  test("table_enum_and_str_types_should_match", () => {
    const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
      projectConfig: { warehouse: "bigquery" },
      tables: [
        {
          target: { schema: "schema", name: "a" },
          enumType: dataform.TableType.TABLE,
          type: "incremental"
        }
      ]
    });

    expect(() => new Builder(graph, {}, TEST_STATE)).to.throw(
      /Table str type "incremental" and enumType "table" are not equivalent/
    );
  });

  suite("pre and post ops", () => {
    const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
      projectConfig: { warehouse: "bigquery" },
      tables: [
        {
          target: { schema: "schema", name: "a" },
          type: "incremental",
          query: "foo",
          incrementalQuery: "incremental foo",
          preOps: ["preOp"],
          incrementalPreOps: ["incremental preOp"],
          postOps: ["postOp"],
          incrementalPostOps: ["incremental postOp"]
        }
      ],
      dataformCoreVersion: "1.4.9"
    });

    test("bigquery when running non incrementally", () => {
      const action = new Builder(graph, {}, TEST_STATE).build().actions[0];
      expect(action.tasks).eql([
        dataform.ExecutionTask.create({
          type: "statement",
          statement: "preOp;\ncreate or replace table `schema.a` as foo;\npostOp"
        })
      ]);
    });

    test("bigquery when running incrementally", () => {
      const action = new Builder(
        graph,
        {},
        dataform.WarehouseState.create({
          tables: [{ target: graph.tables[0].target, fields: [] }]
        })
      ).build().actions[0];
      expect(action.tasks).eql([
        dataform.ExecutionTask.create({
          type: "statement",
          statement:
            "incremental preOp;\ndrop view if exists `schema.a`;\ninsert into `schema.a`\t\n()\t\nselect \t\nfrom (incremental foo) as insertions;\nincremental postOp"
        })
      ]);
    });
  });
});
