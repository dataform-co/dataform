import { config, expect } from "chai";

import { prune } from "df/cli/api";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TEST_GRAPH } from "df/tests/api/utils";

config.truncateThreshold = 0;

suite("@dataform/api/prune", () => {
  //        +-> op_b
  // op_a +-+
  //        +-> op_c
  //
  // op_d +---> tab_a
  const TEST_GRAPH_WITH_TAGS: dataform.ICompiledGraph = dataform.CompiledGraph.create({
    projectConfig: { warehouse: "bigquery", defaultLocation: "US" },
    operations: [
      {
        target: { schema: "schema", name: "op_a" },
        tags: ["tag1"],
        queries: ["create or replace view schema.someview as select 1 as test"]
      },
      {
        target: { schema: "schema", name: "op_b" },
        dependencyTargets: [{ schema: "schema", name: "op_a" }],
        tags: ["tag2"],
        queries: ["create or replace view schema.someview as select 1 as test"]
      },
      {
        target: { schema: "schema", name: "op_c" },
        dependencyTargets: [{ schema: "schema", name: "op_a" }],
        tags: ["tag3"],
        queries: ["create or replace view schema.someview as select 1 as test"]
      },
      {
        target: { schema: "schema", name: "op_d" },
        tags: ["tag3"],
        queries: ["create or replace view schema.someview as select 1 as test"]
      }
    ],
    tables: [
      {
        target: { schema: "schema", name: "tab_a" },
        dependencyTargets: [{ schema: "schema", name: "op_d" }],
        tags: ["tag1", "tag2"]
      }
    ]
  });

  test("prune actions with --tags (with dependencies)", () => {
    const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
      actions: ["op_b", "op_d"],
      tags: ["tag1", "tag2", "tag4"],
      includeDependencies: true
    });
    const actionNames = extractActionNames(prunedGraph);
    expect(actionNames).includes("schema.op_a");
    expect(actionNames).includes("schema.op_b");
    expect(actionNames).not.includes("schema.op_c");
    expect(actionNames).includes("schema.op_d");
    expect(actionNames).includes("schema.tab_a");
  });

  test("prune actions with --tags (with dependents)", () => {
    const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
      tags: ["tag2"],
      includeDependents: true
    });
    const actionNames = extractActionNames(prunedGraph);
    expect(actionNames).not.includes("schema.op_a");
    expect(actionNames).includes("schema.op_b");
    expect(actionNames).not.includes("schema.op_c");
    expect(actionNames).not.includes("schema.op_d");
    expect(actionNames).includes("schema.tab_a");
  });

  test("prune actions with dependents", () => {
    const prunedGraph = prune(TEST_GRAPH, {
      actions: ["schema.c"],
      includeDependents: true
    });
    const actionNames = extractActionNames(prunedGraph);
    expect(actionNames).includes("schema.a");
    expect(actionNames).includes("schema.b");
    expect(actionNames).includes("schema.c");
  });

  test("prune actions with --tags but without --actions (without dependencies or dependents)", () => {
    const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
      tags: ["tag1", "tag2", "tag4"],
      includeDependencies: false,
      includeDependents: false
    });
    const actionNames = extractActionNames(prunedGraph);
    expect(actionNames).includes("schema.op_a");
    expect(actionNames).includes("schema.op_b");
    expect(actionNames).not.includes("schema.op_c");
    expect(actionNames).not.includes("schema.op_d");
    expect(actionNames).includes("schema.tab_a");
  });

  test("prune actions with --actions with dependencies", () => {
    const prunedGraph = prune(TEST_GRAPH, { actions: ["schema.a"], includeDependencies: true });
    const actionNames = extractActionNames(prunedGraph);
    expect(actionNames).includes("schema.a");
    expect(actionNames).includes("schema.b");
  });

  test("prune actions with --actions without dependencies", () => {
    const prunedGraph = prune(TEST_GRAPH, { actions: ["schema.a"], includeDependencies: false });
    const actionNames = extractActionNames(prunedGraph);
    expect(actionNames).includes("schema.a");
    expect(actionNames).not.includes("schema.b");
    expect(actionNames).not.includes("schema.d");
  });
});

function extractActionNames(graph: dataform.ICompiledGraph): string[] {
  return [
    ...(graph.tables ? graph.tables.map(action => targetAsReadableString(action.target)) : []),
    ...(graph.operations
      ? graph.operations.map(action => targetAsReadableString(action.target))
      : []),
    ...(graph.assertions
      ? graph.assertions.map(action => targetAsReadableString(action.target))
      : [])
  ];
}
