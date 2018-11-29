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
        },
        {
          name: "e",
          target: {
            schema: "schema",
            name: "e"
          },
          query: "query",
          dependencies: ["f"],
          disabled: false
        },
        {
          name: "f",
          target: {
            schema: "schema",
            name: "f"
          },
          query: "query",
          dependencies: ["g"],
          disabled: true
        },
        {
          name: "g",
          target: {
            schema: "schema",
            name: "g"
          },
          query: "query",
          disabled: false
        },
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
      const nodeC = executionGraph.nodes.find(n => (n.name === 'c'));
      const nodeD = executionGraph.nodes.find(n => (n.name === 'd'));
      const nodeE = executionGraph.nodes.find(n => (n.name === 'e'));
      const nodeF = executionGraph.nodes.find(n => (n.name === 'f'));
      const nodeG = executionGraph.nodes.find(n => (n.name === 'g'));

      assert.exists(nodeA);
      assert.exists(nodeC);
      assert.exists(nodeD);
      assert.exists(nodeE);
      assert.exists(nodeF);
      assert.exists(nodeG);

      expect(nodeA).to.have.property('tasks').to.be.an('array').that.not.is.empty;
      expect(nodeC).to.have.property('tasks').to.be.an('array').that.is.empty;
      expect(nodeD).to.have.property('tasks').to.be.an('array').that.is.empty;
      expect(nodeE).to.have.property('tasks').to.be.an('array').that.not.is.empty;
      expect(nodeF).to.have.property('tasks').to.be.an('array').that.is.empty;
      expect(nodeG).to.have.property('tasks').to.be.an('array').that.not.is.empty;
    });
  });
});
