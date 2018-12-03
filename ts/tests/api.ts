import { expect, assert } from "chai";

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
          query: "query",
          dependencies: ["c"],
          disabled: true
        },
        {
          name: "c",
          target: {
            schema: "schema",
            name: "c"
          },
          query: "query"
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
      const builder = new Builder(TEST_GRAPH, { includeDependencies: true }, TEST_STATE);
      const executionGraph = builder.build();

      const nodeA = executionGraph.nodes.find(n => (n.name === 'a'));
      const nodeB = executionGraph.nodes.find(n => (n.name === 'b'));
      const nodeC = executionGraph.nodes.find(n => (n.name === 'c'));

      assert.exists(nodeA);
      assert.exists(nodeB);
      assert.exists(nodeC);

      expect(nodeA).to.have.property('tasks').to.be.an('array').that.not.is.empty;
      expect(nodeB).to.have.property('tasks').to.be.an('array').that.is.empty;
      expect(nodeC).to.have.property('tasks').to.be.an('array').that.not.is.empty;
    });
  });
});
