import { expect, assert } from "chai";
import * as rimraf from "rimraf";
import { query, Builder, compile, init } from "@dataform/api";
import * as protos from "@dataform/protos";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import * as stackTrace from "stack-trace";
import { asPlainObject, cleanSql } from "./utils";

describe("@dataform/api", () => {
  describe("build", () => {
    const TEST_GRAPH: protos.ICompiledGraph = protos.CompiledGraph.create({
      projectConfig: { warehouse: "redshift" },
      tables: [
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

    it("build_with_errors", () => {
      expect(() => {
        const graphWithErrors: protos.ICompiledGraph = protos.CompiledGraph.create({
          projectConfig: { warehouse: "redshift" },
          validationErrors: [{ fileName: "someFile", message: "Some critical error" }],
          tables: [{ name: "a", target: { schema: "schema", name: "a" } }]
        });

        const builder = new Builder(graphWithErrors, {}, TEST_STATE);
        builder.build();
      }).to.throw();
    });

    it("node_types", () => {
      const graph: protos.ICompiledGraph = protos.CompiledGraph.create({
        projectConfig: { warehouse: "redshift" },
        tables: [
          { name: "a", target: { schema: "schema", name: "a" }, type: "table" },
          { name: "b", target: { schema: "schema", name: "b" }, type: "incremental" },
          { name: "c", target: { schema: "schema", name: "c" }, type: "view" }
        ],
        operations: [
          {
            name: "d",
            target: { schema: "schema", name: "d" },
            queries: ["create or replace view schema.someview as select 1 as test"]
          }
        ],
        assertions: [{ name: "e" }]
      });

      const builder = new Builder(graph, {}, TEST_STATE);
      const executedGraph = builder.build();

      expect(executedGraph)
        .to.have.property("nodes")
        .to.be.an("array").that.is.not.empty;

      graph.tables.forEach(t => {
        const node = executedGraph.nodes.find(item => item.name == t.name);
        expect(node).to.include({ type: "table", target: t.target, tableType: t.type });
      });

      graph.operations.forEach(t => {
        const node = executedGraph.nodes.find(item => item.name == t.name);
        expect(node).to.include({ type: "operation", target: t.target });
      });

      graph.assertions.forEach(t => {
        const node = executedGraph.nodes.find(item => item.name == t.name);
        expect(node).to.include({ type: "assertion" });
      });
    });
  });

  describe("sql_generating", () => {
    it("bigquery_incremental", () => {
      const graph = protos.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery" },
        tables: [
          {
            name: "incremental",
            target: {
              schema: "schema",
              name: "incremental"
            },
            type: "incremental",
            query: "select 1 as test",
            where: "true"
          }
        ]
      });
      const state = protos.WarehouseState.create({
        tables: [
          {
            target: {
              schema: "schema",
              name: "incremental"
            },
            type: "table",
            fields: [
              {
                name: "existing_field"
              }
            ]
          }
        ]
      });
      const executionGraph = new Builder(graph, {}, state).build();
      expect(executionGraph.nodes.filter(n => n.name == "incremental")).is.not.empty;
      expect(cleanSql(executionGraph.nodes.filter(n => n.name == "incremental")[0].tasks[0].statement)).equals(
        cleanSql(
          `insert into \`schema.incremental\` (existing_field)
           select existing_field from (
             select * from (select 1 as test)
             where true
           )`
        )
      );
    });

    it("redshift_create", () => {
      const testGraph: protos.ICompiledGraph = protos.CompiledGraph.create({
        projectConfig: { warehouse: "redshift" },
        tables: [
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

    it("bigquery_partitionby", () => {
      const testGraph: protos.ICompiledGraph = protos.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery" },
        tables: [
          {
            name: "partitionby",
            target: {
              schema: "schema",
              name: "name"
            },
            type: "table",
            query: "select 1 as test",
            bigquery: {
              partitionBy: "DATE(test)"
            }
          },
          {
            name: "plain",
            target: {
              schema: "schema",
              name: "name"
            },
            type: "table",
            query: "select 1 as test"
          }
        ]
      });
      const expectedExecutionNodes: protos.IExecutionNode[] = [
        {
          name: "partitionby",
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "name"
          },
          tasks: [
            {
              type: "statement",
              statement: "create or replace table `schema.name` partition by DATE(test) as select 1 as test"
            }
          ]
        },
        {
          name: "plain",
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "name"
          },
          tasks: [
            {
              type: "statement",
              statement: "create or replace table `schema.name`  as select 1 as test"
            }
          ]
        }
      ];
      const executionGraph = new Builder(testGraph, {}, protos.WarehouseState.create({})).build();
      expect(asPlainObject(executionGraph.nodes)).deep.equals(asPlainObject(expectedExecutionNodes));
    });

    it("snowflake", () => {
      const testGraph: protos.ICompiledGraph = protos.CompiledGraph.create({
        projectConfig: { warehouse: "snowflake" },
        tables: [
          {
            name: "a",
            target: {
              schema: "schema",
              name: "a"
            },
            query: "select 1 as test"
          },
          {
            name: "b",
            target: {
              schema: "schema",
              name: "b"
            },
            dependencies: ["a"],
            query: "select 1 as test"
          }
        ]
      });
      const testState = protos.WarehouseState.create({});
      const builder = new Builder(testGraph, {}, testState);
      const executionGraph = builder.build();

      expect(executionGraph.nodes)
        .to.be.an("array")
        .to.have.lengthOf(2);

      executionGraph.nodes.forEach((node, index) => {
        expect(node)
          .to.have.property("tasks")
          .to.be.an("array").that.is.not.empty;

        const statements = node.tasks.map(item => item.statement);
        expect(statements).includes(`create or replace table "schema"."${node.name}" as select 1 as test`);
      });
    });
  });

  describe("init", () => {
    let projectDir;

    after(() => {
      // delete project directory
      if (fs.existsSync(projectDir)) {
        rimraf.sync(projectDir);
      }
    });

    it("redshift", async function() {
      this.timeout(30000);

      // create temp directory
      projectDir = fs.mkdtempSync(path.join(os.tmpdir(), "df-"));

      // init new project
      await init(projectDir, {
        warehouse: "redshift"
      });

      // add new table
      const query = "select 1 as test";
      const mPath = path.resolve(projectDir, "./definitions/simplemodel.sql");
      fs.writeFileSync(mPath, query);

      expect(fs.existsSync(mPath)).to.be.true;

      // compile project
      const graph = await compile(projectDir).catch(error => error);
      expect(graph).to.not.be.an.instanceof(Error);

      expect(graph)
        .to.have.property("compileErrors")
        .to.be.an("array").that.is.empty;
      expect(graph)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;
      expect(graph)
        .to.have.property("tables")
        .to.be.an("array").that.is.not.empty;

      graph.tables.forEach(item => {
        expect(item).to.satisfy(t => !t.validationErrors || !t.validationErrors.length);
      });
    });
  });

  describe("compile", () => {
    it("bigquery_example", () => {
      return compile("../examples/bigquery").then(graph => {
        var tableNames = graph.tables.map(t => t.name);

        // Check JS blocks get processed.
        expect(tableNames).includes("example_js_blocks");
        var exampleJsBlocks = graph.tables.filter(t => t.name == "example_js_blocks")[0];
        expect(exampleJsBlocks.type).equals("table");
        expect(exampleJsBlocks.query).equals("select 1 as foo");

        // Check we can import and use an external package.
        expect(tableNames).includes("example_incremental");
        var exampleIncremental = graph.tables.filter(t => t.name == "example_incremental")[0];
        expect(exampleIncremental.query).equals("select current_timestamp() as ts");

        // Check tables defined in includes are not included.
        expect(tableNames).not.includes("example_ignore");

        // Check SQL files with raw back-ticks get escaped.
        expect(tableNames).includes("example_backticks");
        var exampleBackticks = graph.tables.filter(t => t.name == "example_backticks")[0];
        expect(cleanSql(exampleBackticks.query)).equals(
          "select * from `tada-analytics.df_integration_test.sample_data`"
        );

        // Check deferred calls to table resolve to the correct definitions file.
        expect(tableNames).includes("example_deferred");
        var exampleDeferred = graph.tables.filter(t => t.name == "example_deferred")[0];
        expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");
      });
    });

    it("redshift_example", () => {
      return compile("../examples/redshift").then(graph => {
        var tableNames = graph.tables.map(t => t.name);

        // Check we can import and use an external package.
        expect(tableNames).includes("example_incremental");
        var exampleIncremental = graph.tables.filter(t => t.name == "example_incremental")[0];
        expect(exampleIncremental.query).equals("select current_timestamp::timestamp as ts");
      });
    });

    it("bigquery_with_errors_example", async () => {
      const expectedResults = [
        {
          fileName: "definitions/test.js",
          message: /Error in JS/,
          lineNumber: 4
        },
        {
          fileName: "definitions/example_js_blocks.sql",
          message: /Error in multiline comment/
        },
        {
          fileName: "definitions/example_table.sql",
          message: /ref_with_error is not defined/
        }
      ];
      const graph = await compile("../examples/bigquery_with_errors").catch(error => error);

      expect(graph).to.not.be.an.instanceof(Error);
      expect(graph)
        .to.have.property("compileErrors")
        .to.be.an("array");

      expectedResults.forEach(result => {
        const error = graph.compileErrors.find(item => item.message.match(result.message));

        expect(error)
          .to.have.property("fileName")
          .that.equals(result.fileName);

        expect(error)
          .to.have.property("stack")
          .that.is.a("string");

        if (result.lineNumber) {
          const stack = stackTrace.parse(error);
          expect(stack).to.be.an("array").that.is.not.empty;
          expect(stack[0])
            .to.have.property("lineNumber")
            .that.equals(result.lineNumber);
        }
      });
    });

    it("bigquery_backwards_compatibility_example", () => {
      return compile("../examples/bigquery_backwards_compatibility").then(graph => {
        const tableNames = graph.tables.map(t => t.name);

        // We just want to make sure this compiles really.
        expect(tableNames).includes("example");
        const example = graph.tables.filter(t => t.name == "example")[0];
        expect(example.type).equals("table");
        expect(example.query.trim()).equals("select 1 as foo_bar");
      });
    });

    it("operation_refing", async function() {
      const expectedQueries = {
        sample_1: 'create table "test_schema"."sample_1" as select 1 as sample_1',
        sample_2: 'select * from "test_schema"."sample_1"'
      };

      const graph = await compile("../examples/redshift_operations").catch(error => error);
      expect(graph).to.not.be.an.instanceof(Error);

      expect(graph)
        .to.have.property("compileErrors")
        .to.be.an("array").that.is.empty;
      expect(graph)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;
      expect(graph)
        .to.have.property("operations")
        .to.be.an("array")
        .that.have.lengthOf(Object.keys(expectedQueries).length);

      graph.operations.forEach(op => {
        expect(op.queries).deep.equals([expectedQueries[op.name]]);
      });
    });

    it("snowflake_example", async () => {
      const graph = await compile("../examples/snowflake").catch(error => error);
      expect(graph).to.not.be.an.instanceof(Error);

      expect(graph)
        .to.have.property("compileErrors")
        .to.be.an("array").that.is.empty;
      expect(graph)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;

      const mNames = graph.tables.map(t => t.name);

      expect(mNames).includes("example_incremental");
      const mIncremental = graph.tables.filter(t => t.name == "example_incremental")[0];
      expect(mIncremental.type).equals("incremental");
      expect(mIncremental.query).equals("select convert_timezone('UTC', current_timestamp())::timestamp as ts");
      expect(mIncremental.dependencies).to.be.an("array").that.is.empty;

      expect(mNames).includes("example_table");
      const mTable = graph.tables.filter(t => t.name == "example_table")[0];
      expect(mTable.type).equals("table");
      expect(mTable.query).equals('\nselect * from "df_integration_test"."sample_data"');
      expect(mTable.dependencies).deep.equals(["sample_data"]);

      expect(mNames).includes("example_view");
      const mView = graph.tables.filter(t => t.name == "example_view")[0];
      expect(mView.type).equals("view");
      expect(mView.query).equals('\nselect * from "df_integration_test"."sample_data"');
      expect(mView.dependencies).deep.equals(["sample_data"]);

      expect(mNames).includes("sample_data");
      const mSampleData = graph.tables.filter(t => t.name == "sample_data")[0];
      expect(mSampleData.type).equals("view");
      expect(mSampleData.query).equals(
        "select 1 as sample_column union all\nselect 2 as sample_column union all\nselect 3 as sample_column"
      );
      expect(mSampleData.dependencies).to.be.an("array").that.is.empty;

      const aNames = graph.assertions.map(a => a.name);

      expect(aNames).includes("sample_data_assertion");
      const assertion = graph.assertions.filter(a => a.name == "sample_data_assertion")[0];
      expect(assertion.query).equals('select * from "df_integration_test"."sample_data" where sample_column > 3');
      expect(assertion.dependencies).to.include.members([
        "sample_data",
        "example_table",
        "example_incremental",
        "example_view"
      ]);
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
