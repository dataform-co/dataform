import { expect, assert } from "chai";
import { compile } from "@dataform/api";
import { query, Builder } from "@dataform/api";
import * as protos from "@dataform/protos";
import { throws } from "assert";

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

      const nodeA = executionGraph.nodes.find(n => n.name === "a");
      const nodeB = executionGraph.nodes.find(n => n.name === "b");
      const nodeC = executionGraph.nodes.find(n => n.name === "c");

      assert.exists(nodeA);
      assert.exists(nodeB);
      assert.exists(nodeC);

      expect(nodeA)
        .to.have.property("tasks")
        .to.be.an("array").that.not.is.empty;
      expect(nodeB)
        .to.have.property("tasks")
        .to.be.an("array").that.is.empty;
      expect(nodeC)
        .to.have.property("tasks")
        .to.be.an("array").that.not.is.empty;
    });
  });

  describe("sql_generating", () => {
    it("redshift_create", () => {
      const testGraph: protos.ICompiledGraph = protos.CompiledGraph.create({
        projectConfig: { warehouse: "redshift" },
        materializations: [
          {
            name: "redshift_all",
            target: {
              schema: "schema",
              name: "redshift_all"
            },
            query: "query",
            redshift: {
              distKey: "column1",
              distStyle: "even",
              sortKeys: ["column1", "column2"],
              sortStyle: "compound"
            }
          },
          {
            name: "redshift_only_sort",
            target: {
              schema: "schema",
              name: "redshift_only_sort"
            },
            query: "query",
            redshift: {
              sortKeys: ["column1"],
              sortStyle: "interleaved"
            }
          },
          {
            name: "redshift_only_dist",
            target: {
              schema: "schema",
              name: "redshift_only_dist"
            },
            query: "query",
            redshift: {
              distKey: "column1",
              distStyle: "even"
            }
          },
          {
            name: "redshift_empty_redshift",
            target: {
              schema: "schema",
              name: "redshift_empty_redshift"
            },
            query: "query",
            redshift: {}
          },
          {
            name: "redshift_without_redshift",
            target: {
              schema: "schema",
              name: "redshift_without_redshift"
            },
            query: "query"
          }
        ]
      });
      const testState = protos.WarehouseState.create({});
      const expectedSQL = [
        'create table "schema"."redshift_all_temp" diststyle even distkey (column1) compound sortkey (column1, column2) as query',
        'create table "schema"."redshift_only_sort_temp" interleaved sortkey (column1) as query',
        'create table "schema"."redshift_only_dist_temp" diststyle even distkey (column1) as query',
        'create table "schema"."redshift_empty_redshift_temp" as query',
        'create table "schema"."redshift_without_redshift_temp" as query'
      ];

      const builder = new Builder(testGraph, {}, testState);
      const executionGraph = builder.build();

      expect(executionGraph.nodes)
        .to.be.an("array")
        .to.have.lengthOf(5);

      executionGraph.nodes.forEach((node, index) => {
        expect(node)
          .to.have.property("tasks")
          .to.be.an("array").that.is.not.empty;

        const statements = node.tasks.map(item => item.statement);
        expect(statements).includes(expectedSQL[index]);
      });
    });
  });

  describe("compile", () => {
    it("bigquery_example", () => {
      return compile("../examples/bigquery").then(graph => {
        var materializationNames = graph.materializations.map(m => m.name);

        // Check JS blocks get processed.
        expect(materializationNames).includes("example_js_blocks");
        var exampleJsBlocks = graph.materializations.filter(m => m.name == "example_js_blocks")[0];
        expect(exampleJsBlocks.type).equals("table");
        expect(exampleJsBlocks.query).equals("select 1 as foo");

        // Check we can import and use an external package.
        expect(materializationNames).includes("example_incremental");
        var exampleIncremental = graph.materializations.filter(m => m.name == "example_incremental")[0];
        expect(exampleIncremental.query).equals("select current_timestamp() as ts");

        // Check materializations defined in includes are not included.
        expect(materializationNames).not.includes("example_ignore");
      });
    });

    it("redshift_example", () => {
      return compile("../examples/redshift").then(graph => {
        var materializationNames = graph.materializations.map(m => m.name);

        // Check we can import and use an external package.
        expect(materializationNames).includes("example_incremental");
        var exampleIncremental = graph.materializations.filter(m => m.name == "example_incremental")[0];
        expect(exampleIncremental.query).equals("select current_timestamp::timestamp as ts");
      });
    });

    it("bigquery_with_errors_example", async () => {
      const graph = await compile("../examples/bigquery_with_errors").catch(error => error);

      expect(graph).to.not.be.an.instanceof(Error);
      expect(graph)
        .to.have.property("compileErrors")
        .to.be.an("array");

      const errors1 = graph.compileErrors.filter(item => item.message.match(/ref_with_error is not defined/));
      expect(errors1).to.be.an("array").that.is.not.empty;

      const errors2 = graph.compileErrors.filter(item => item.message.match(/Error in multiline comment/));
      expect(errors2).to.be.an("array").that.is.not.empty;

      const errors3 = graph.compileErrors.filter(item => item.message.match(/Error in JS/));
      expect(errors3).to.be.an("array").that.is.not.empty;
    });
  });

  describe("query", () => {
    it("bigquery_example", () => {
      return query.compile('select 1 as ${describe("test")}', "../examples/bigquery").then(compiledQuery => {
        expect(compiledQuery).equals("select 1 as test");
      });
    });
  });
});
