import { expect } from "chai";

import { Builder } from "@dataform/api/commands/build";
import * as protos from "@dataform/protos";

describe("@dataform/api", () => {
  describe("build", () => {
    const TEST_GRAPH: protos.ICompiledGraph = protos.CompiledGraph.create({
      projectConfig: { warehouse: "redshift" },
      materializations: [
        {
          name: "a",
          target: {
            schema: "schema",
            name: "a"
          },
          query: "query",
          dependencies: ["b"]
        },
        {
          name: "b",
          target: {
            schema: "schema",
            name: "b"
          },
          query: "query"
        },
        {
          name: "c",
          target: {
            schema: "schema",
            name: "c"
          },
          query: "query",
          dependencies: ["d"],
          disabled: true
        },
        {
          name: "d",
          target: {
            schema: "schema",
            name: "d"
          },
          query: "query",
          disabled: true
        }
      ]
    });

    const TEST_STATE = protos.WarehouseState.create({ tables: [] });

    it("include_deps", () => {
      var builder = new Builder(TEST_GRAPH, { nodes: ["a"], includeDependencies: true }, TEST_STATE);
      var executionGraph = builder.build();
      var includedNodeNames = executionGraph.nodes.map(n => n.name);
      expect(includedNodeNames).includes("a");
      expect(includedNodeNames).includes("b");
    });

    it("exclude_deps", () => {
      var builder = new Builder(TEST_GRAPH, { nodes: ["a"], includeDependencies: false }, TEST_STATE);
      var executionGraph = builder.build();
      var includedNodeNames = executionGraph.nodes.map(n => n.name);
      expect(includedNodeNames).includes("a");
      expect(includedNodeNames).not.includes("b");
    });

    it("exclude_disabled", () => {
      var builder = new Builder(TEST_GRAPH, { includeDependencies: true }, TEST_STATE);
      var executionGraph = builder.build();
      var includedNodeNames = executionGraph.nodes.map(n => n.name);
      expect(includedNodeNames).includes("a");
      expect(includedNodeNames).includes("b");
      expect(includedNodeNames).not.includes("c");
      expect(includedNodeNames).not.includes("d");
    });
  });
});
