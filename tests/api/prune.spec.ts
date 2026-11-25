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
        queries: ["create or replace view schema.someview as select 1 as test"],
      },
      {
        target: { schema: "schema", name: "op_b" },
        dependencyTargets: [{ schema: "schema", name: "op_a" }],
        tags: ["tag2"],
        queries: ["create or replace view schema.someview as select 1 as test"],
      },
      {
        target: { schema: "schema", name: "op_c" },
        dependencyTargets: [{ schema: "schema", name: "op_a" }],
        tags: ["tag3"],
        queries: ["create or replace view schema.someview as select 1 as test"],
      },
      {
        target: { schema: "schema", name: "op_d" },
        tags: ["tag3"],
        queries: ["create or replace view schema.someview as select 1 as test"],
      },
    ],
    tables: [
      {
        target: { schema: "schema", name: "tab_a" },
        dependencyTargets: [{ schema: "schema", name: "op_d" }],
        tags: ["tag1", "tag2"],
      },
    ],
  });

  test("prune actions with --tags (with dependencies)", () => {
    const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
      actions: ["op_b", "op_d"],
      tags: ["tag1", "tag2", "tag4"],
      includeDependencies: true,
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
      includeDependents: true,
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
      includeDependents: true,
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
      includeDependents: false,
    });
    const actionNames = extractActionNames(prunedGraph);
    expect(actionNames).includes("schema.op_a");
    expect(actionNames).includes("schema.op_b");
    expect(actionNames).not.includes("schema.op_c");
    expect(actionNames).not.includes("schema.op_d");
    expect(actionNames).includes("schema.tab_a");
  });

  suite("--tags-intersection", () => {
    test("selects only actions with every tag", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        tags: ["tag1", "tag2"],
        tagsIntersection: true,
      });
      expect(extractActionNames(prunedGraph)).deep.equals(["schema.tab_a"]);
    });

    test("without the flag the same tags are a union (unchanged default)", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, { tags: ["tag1", "tag2"] });
      expect(extractActionNames(prunedGraph).sort()).deep.equals([
        "schema.op_a",
        "schema.op_b",
        "schema.tab_a",
      ]);
    });

    test("a single tag selects the same actions as without the flag", () => {
      const withFlag = prune(TEST_GRAPH_WITH_TAGS, { tags: ["tag3"], tagsIntersection: true });
      const withoutFlag = prune(TEST_GRAPH_WITH_TAGS, { tags: ["tag3"] });
      expect(extractActionNames(withFlag)).deep.equals(extractActionNames(withoutFlag));
    });

    test("a tag no action has selects nothing", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        tags: ["tag1", "tag4"],
        tagsIntersection: true,
      });
      expect(extractActionNames(prunedGraph)).deep.equals([]);
    });

    test("tags are matched literally, including an empty tag", () => {
      // Core accepts "" as a tag, and union mode already matches it literally.
      const graph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultLocation: "US" },
        operations: [
          { target: { schema: "schema", name: "empty" }, tags: [""] },
          { target: { schema: "schema", name: "empty_and_a" }, tags: ["", "a"] },
          { target: { schema: "schema", name: "only_a" }, tags: ["a"] },
        ],
      });
      // "--tags a," splits into ["a", ""].
      expect(extractActionNames(prune(graph, { tags: ["a", ""] })).sort()).deep.equals([
        "schema.empty",
        "schema.empty_and_a",
        "schema.only_a",
      ]);
      expect(
        extractActionNames(prune(graph, { tags: ["a", ""], tagsIntersection: true })),
      ).deep.equals(["schema.empty_and_a"]);
    });

    test("--actions is still added on top of the tag intersection", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        actions: ["op_c"],
        tags: ["tag1", "tag2"],
        tagsIntersection: true,
      });
      expect(extractActionNames(prunedGraph).sort()).deep.equals(["schema.op_c", "schema.tab_a"]);
    });

    test("--include-deps expands from the intersection, not from each tag", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        tags: ["tag1", "tag2"],
        tagsIntersection: true,
        includeDependencies: true,
      });
      // op_d has neither tag1 nor tag2, but is an upstream dependency of tab_a.
      expect(extractActionNames(prunedGraph).sort()).deep.equals(["schema.op_d", "schema.tab_a"]);
    });

    test("--include-dependents adds untagged downstream actions of the intersection", () => {
      //   both (t1, t2) -> both_child (untagged)
      //   only_t1 (t1)  -> only_t1_child (untagged)
      const graph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultLocation: "US" },
        operations: [
          { target: { schema: "schema", name: "both" }, tags: ["t1", "t2"] },
          {
            target: { schema: "schema", name: "both_child" },
            dependencyTargets: [{ schema: "schema", name: "both" }],
          },
          { target: { schema: "schema", name: "only_t1" }, tags: ["t1"] },
          {
            target: { schema: "schema", name: "only_t1_child" },
            dependencyTargets: [{ schema: "schema", name: "only_t1" }],
          },
        ],
      });
      const prunedGraph = prune(graph, {
        tags: ["t1", "t2"],
        tagsIntersection: true,
        includeDependents: true,
      });
      expect(extractActionNames(prunedGraph).sort()).deep.equals([
        "schema.both",
        "schema.both_child",
      ]);
    });
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
    ...(graph.tables ? graph.tables.map((action) => targetAsReadableString(action.target)) : []),
    ...(graph.operations
      ? graph.operations.map((action) => targetAsReadableString(action.target))
      : []),
    ...(graph.assertions
      ? graph.assertions.map((action) => targetAsReadableString(action.target))
      : []),
  ];
}
