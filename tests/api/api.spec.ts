import { Builder, compile, init, query, Runner, utils as apiUtils } from "@dataform/api";
import { DbAdapter } from "@dataform/api/dbadapters";
import { BigQueryDbAdapter } from "@dataform/api/dbadapters/bigquery";
import { utils } from "@dataform/core";
import { dataform } from "@dataform/protos";
import { assert, config, expect } from "chai";
import { asPlainObject, cleanSql } from "df/tests/utils";
import * as path from "path";
import * as rimraf from "rimraf";
import * as stackTrace from "stack-trace";
import { anyFunction, anyString, instance, mock, when } from "ts-mockito";

config.truncateThreshold = 0;

describe("@dataform/api", () => {
  describe("build", () => {
    const TEST_GRAPH: dataform.ICompiledGraph = dataform.CompiledGraph.create({
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

    const TEST_STATE = dataform.WarehouseState.create({ tables: [] });

    it("include_deps", () => {
      const builder = new Builder(
        TEST_GRAPH,
        { nodes: ["a"], includeDependencies: true },
        TEST_STATE
      );
      const executionGraph = builder.build();
      const includedNodeNames = executionGraph.nodes.map(n => n.name);
      expect(includedNodeNames).includes("a");
      expect(includedNodeNames).includes("b");
    });

    it("exclude_deps", () => {
      const builder = new Builder(
        TEST_GRAPH,
        { nodes: ["a"], includeDependencies: false },
        TEST_STATE
      );
      const executionGraph = builder.build();
      const includedNodeNames = executionGraph.nodes.map(n => n.name);
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
        const graphWithErrors: dataform.ICompiledGraph = dataform.CompiledGraph.create({
          projectConfig: { warehouse: "redshift" },
          graphErrors: { compilationErrors: [{ message: "Some critical error" }] },
          tables: [{ name: "a", target: { schema: "schema", name: "a" } }]
        });

        const builder = new Builder(graphWithErrors, {}, TEST_STATE);
        builder.build();
      }).to.throw();
    });

    it("node_types", () => {
      const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "redshift" },
        tables: [
          { name: "a", target: { schema: "schema", name: "a" }, type: "table" },
          {
            name: "b",
            target: { schema: "schema", name: "b" },
            type: "incremental",
            where: "test"
          },
          { name: "c", target: { schema: "schema", name: "c" }, type: "view" }
        ],
        operations: [
          {
            name: "d",
            target: { schema: "schema", name: "d" },
            queries: ["create or replace view schema.someview as select 1 as test"]
          }
        ],
        assertions: [{ name: "e", target: { schema: "schema", name: "d" } }]
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

    it("inline_tables", () => {
      const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery" },
        tables: [
          { name: "a", target: { schema: "schema", name: "a" }, type: "table", dependencies: [] },
          {
            name: "b",
            target: { schema: "schema", name: "b" },
            type: "inline",
            dependencies: ["a"]
          },
          { name: "c", target: { schema: "schema", name: "c" }, type: "table", dependencies: ["a"] }
        ]
      });

      const builder = new Builder(graph, {}, TEST_STATE);
      const executedGraph = builder.build();

      expect(executedGraph).to.exist;
      expect(executedGraph)
        .to.have.property("nodes")
        .to.be.an("array").that.is.not.empty;

      const nodeNames = executedGraph.nodes.map(t => t.name);

      expect(nodeNames).includes("a");
      expect(nodeNames).not.includes("b");
      expect(nodeNames).includes("c");
    });
  });

  describe("sql_generating", () => {
    it("bigquery_incremental", () => {
      const graph = dataform.CompiledGraph.create({
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
      const state = dataform.WarehouseState.create({
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
      expect(
        cleanSql(executionGraph.nodes.filter(n => n.name == "incremental")[0].tasks[0].statement)
      ).equals(
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
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
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
            name: "redshift_without_redshift",
            target: {
              schema: "schema",
              name: "redshift_without_redshift"
            },
            query: "query"
          }
        ]
      });
      const testState = dataform.WarehouseState.create({});
      const expectedSQL = [
        'create table "schema"."redshift_all_temp" diststyle even distkey (column1) compound sortkey (column1, column2) as query',
        'create table "schema"."redshift_only_sort_temp" interleaved sortkey (column1) as query',
        'create table "schema"."redshift_only_dist_temp" diststyle even distkey (column1) as query',
        'create table "schema"."redshift_without_redshift_temp" as query'
      ];

      const builder = new Builder(testGraph, {}, testState);
      const executionGraph = builder.build();

      expect(executionGraph.nodes)
        .to.be.an("array")
        .to.have.lengthOf(4);

      executionGraph.nodes.forEach((node, index) => {
        expect(node)
          .to.have.property("tasks")
          .to.be.an("array").that.is.not.empty;

        const statements = node.tasks.map(item => item.statement);
        expect(statements).includes(expectedSQL[index]);
      });
    });

    it("bigquery_partitionby", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
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
      const expectedExecutionNodes: dataform.IExecutionNode[] = [
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
              statement:
                "create or replace table `schema.name` partition by DATE(test) as select 1 as test"
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
      const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
      expect(asPlainObject(executionGraph.nodes)).deep.equals(
        asPlainObject(expectedExecutionNodes)
      );
    });

    it("snowflake", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
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
      const testState = dataform.WarehouseState.create({});
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
        expect(statements).includes(
          `create or replace table "schema"."${node.name}" as select 1 as test`
        );
      });
    });
  });

  describe("init", () => {
    it("init", async function() {
      this.timeout(30000);

      // create temp directory
      const projectDir = "df/examples/init";

      // Project has already been initialized via the tests script, check data is valid.

      // compile project
      const graph = await compile({ projectDir }).catch(error => error);
      expect(graph).to.not.be.an.instanceof(Error);

      const gErrors = utils.validate(graph);

      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.empty;
      expect(gErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;
    });
  });

  describe("compile", () => {
    it("bigquery_example", async () => {
      const graph = await compile({ projectDir: path.resolve("df/examples/bigquery") });
      const tableNames = graph.tables.map(t => t.name);

      // Check JS blocks get processed.
      expect(tableNames).includes("example_js_blocks");
      const exampleJsBlocks = graph.tables.filter(t => t.name == "example_js_blocks")[0];
      expect(exampleJsBlocks.type).equals("table");
      expect(exampleJsBlocks.query).equals("select 1 as foo");

      // Check we can import and use an external package.
      expect(tableNames).includes("example_incremental");
      const exampleIncremental = graph.tables.filter(t => t.name == "example_incremental")[0];
      expect(exampleIncremental.query).equals("select current_timestamp() as ts");

      // Check tables defined in includes are not included.
      expect(tableNames).not.includes("example_ignore");

      // Check SQL files with raw back-ticks get escaped.
      expect(tableNames).includes("example_backticks");
      const exampleBackticks = graph.tables.filter(t => t.name == "example_backticks")[0];
      expect(cleanSql(exampleBackticks.query)).equals(
        "select * from `tada-analytics.df_integration_test.sample_data`"
      );

      // Check deferred calls to table resolve to the correct definitions file.
      expect(tableNames).includes("example_deferred");
      const exampleDeferred = graph.tables.filter(t => t.name == "example_deferred")[0];
      expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");

      // Check inline tables
      expect(tableNames).includes("example_inline");
      const exampleInline = graph.tables.filter(t => t.name == "example_inline")[0];
      expect(exampleInline.type).equals("inline");
      expect(exampleInline.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleInline.dependencies).includes("sample_data");

      expect(tableNames).includes("example_using_inline");
      const exampleUsingInline = graph.tables.filter(t => t.name == "example_using_inline")[0];
      expect(exampleUsingInline.type).equals("table");
      expect(exampleUsingInline.query).equals(
        "\nselect * from (\nselect * from `tada-analytics.df_integration_test.sample_data`)\nwhere true"
      );
      expect(exampleUsingInline.dependencies).includes("sample_data");
    });

    it("schema overrides", async () => {
      const graph = await compile({
        projectDir: path.resolve("df/examples/bigquery"),
        schemaSuffixOverride: "suffix"
      });
      expect(graph.projectConfig.schemaSuffix).to.equal("suffix");
      graph.tables.forEach(table =>
        expect(table.target.schema).to.match(
          /^(df_integration_test_suffix|override_schema_suffix)$/
        )
      );
    });

    it("redshift_example", () => {
      return compile({ projectDir: "df/examples/redshift" }).then(graph => {
        const tableNames = graph.tables.map(t => t.name);

        // Check we can import and use an external package.
        expect(tableNames).includes("example_incremental");
        const exampleIncremental = graph.tables.filter(t => t.name == "example_incremental")[0];
        expect(exampleIncremental.query).equals("select current_timestamp::timestamp as ts");

        // Check inline tables
        expect(tableNames).includes("example_inline");
        const exampleInline = graph.tables.filter(t => t.name == "example_inline")[0];
        expect(exampleInline.type).equals("inline");
        expect(exampleInline.query).equals('\nselect * from "df_integration_test"."sample_data"');
        expect(exampleInline.dependencies).includes("sample_data");

        expect(tableNames).includes("example_using_inline");
        const exampleUsingInline = graph.tables.filter(t => t.name == "example_using_inline")[0];
        expect(exampleUsingInline.type).equals("table");
        expect(exampleUsingInline.query).equals(
          '\nselect * from (\nselect * from "df_integration_test"."sample_data")\nwhere true'
        );
        expect(exampleUsingInline.dependencies).includes("sample_data");
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
      const graph = await compile({
        projectDir: path.resolve("df/examples/bigquery_with_errors")
      }).catch(error => error);
      expect(graph).to.not.be.an.instanceof(Error);

      const gErrors = utils.validate(graph);

      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array");

      expectedResults.forEach(result => {
        const error = gErrors.compilationErrors.find(item => result.message.test(item.message));

        expect(error).to.exist;
        expect(error)
          .to.have.property("fileName")
          .that.equals(result.fileName);
        expect(error)
          .to.have.property("stack")
          .that.is.a("string");

        if (result.lineNumber) {
          const err = new Error();
          err.stack = error.stack;
          const stack = stackTrace.parse(err);
          expect(stack).to.be.an("array").that.is.not.empty;
          expect(stack[0])
            .to.have.property("lineNumber")
            .that.equals(result.lineNumber);
        }
      });
    });

    it("bigquery_backwards_compatibility_example", async () => {
      const graph = await compile({ projectDir: "df/examples/bigquery_backwards_compatibility" });

      const tableNames = graph.tables.map(t => t.name);

      // Make sure it compiles.
      expect(tableNames).includes("example");
      const example = graph.tables.filter(t => t.name == "example")[0];
      expect(example.type).equals("table");
      expect(example.query.trim()).equals("select 1 as foo_bar");

      // Make sure we can dry run.
      new Builder(graph, {}, { tables: [] }).build();
    });

    it("operation_refing", async function() {
      const expectedQueries = {
        sample_1: 'create table "test_schema"."sample_1" as select 1 as sample_1',
        sample_2: 'select * from "test_schema"."sample_1"'
      };

      const graph = await compile({ projectDir: "df/examples/redshift_operations" }).catch(
        error => error
      );
      expect(graph).to.not.be.an.instanceof(Error);

      const gErrors = utils.validate(graph);

      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.empty;
      expect(gErrors)
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
      const graph = await compile({ projectDir: "df/examples/snowflake" }).catch(error => error);
      expect(graph).to.not.be.an.instanceof(Error);

      const gErrors = utils.validate(graph);

      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.empty;
      expect(gErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;

      const mNames = graph.tables.map(t => t.name);

      expect(mNames).includes("example_incremental");
      const mIncremental = graph.tables.filter(t => t.name == "example_incremental")[0];
      expect(mIncremental.type).equals("incremental");
      expect(mIncremental.query).equals(
        "select convert_timezone('UTC', current_timestamp())::timestamp as ts"
      );
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

      // Check inline tables
      expect(mNames).includes("example_inline");
      const exampleInline = graph.tables.filter(t => t.name == "example_inline")[0];
      expect(exampleInline.type).equals("inline");
      expect(exampleInline.query).equals('\nselect * from "df_integration_test"."sample_data"');
      expect(exampleInline.dependencies).includes("sample_data");

      expect(mNames).includes("example_using_inline");
      const exampleUsingInline = graph.tables.filter(t => t.name == "example_using_inline")[0];
      expect(exampleUsingInline.type).equals("table");
      expect(exampleUsingInline.query).equals(
        '\nselect * from (\nselect * from "df_integration_test"."sample_data")\nwhere true'
      );
      expect(exampleUsingInline.dependencies).includes("sample_data");

      const aNames = graph.assertions.map(a => a.name);

      expect(aNames).includes("sample_data_assertion");
      const assertion = graph.assertions.filter(a => a.name == "sample_data_assertion")[0];
      expect(assertion.query).equals(
        'select * from "df_integration_test"."sample_data" where sample_column > 3'
      );
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
      return query
        .compile('select 1 as ${describe("test")}', { projectDir: "df/examples/bigquery" })
        .then(compiledQuery => {
          expect(compiledQuery).equals("select 1 as test");
        });
    });
  });

  describe("credentials_config", () => {
    const bigqueryCredentials = { projectId: "", credentials: "" };
    const redshiftCredentials = {
      host: "",
      port: 0,
      username: "",
      password: "",
      databaseName: ""
    };
    const snowflakeCredentials = {
      accountId: "",
      username: "",
      password: "",
      role: "",
      databaseName: "",
      warehouse: ""
    };

    ["bigquery", "redshift", "snowflake"].forEach(warehouse => {
      it(`${warehouse}_empty_credentials`, () => {
        expect(() => apiUtils.coerceCredentials(warehouse, null)).to.throw(
          /Credentials JSON object does not conform to protobuf requirements: object expected/
        );
        expect(() => apiUtils.coerceCredentials(warehouse, {})).to.throw(
          /Missing required properties:/
        );
      });
    });

    it("warehouse_check", () => {
      expect(() => apiUtils.coerceCredentials("bigquery", bigqueryCredentials)).to.not.throw();
      expect(() => apiUtils.coerceCredentials("redshift", redshiftCredentials)).to.not.throw();
      expect(() => apiUtils.coerceCredentials("snowflake", snowflakeCredentials)).to.not.throw();
      expect(() => apiUtils.coerceCredentials("some_other_warehouse", {})).to.throw(
        /Unrecognized warehouse:/
      );
    });

    [{}, { wrongProperty: "" }, { projectId: "" }].forEach(bigquery => {
      it("bigquery_properties_check", () => {
        expect(() =>
          apiUtils.coerceCredentials("bigquery", JSON.parse(JSON.stringify(bigquery)))
        ).to.throw();

        expect(() =>
          apiUtils.coerceCredentials(
            "bigquery",
            JSON.parse(JSON.stringify({ ...bigqueryCredentials, oneMoreProperty: "" }))
          )
        ).to.not.throw(/Missing required properties/);
      });
    });

    [{}, { wrongProperty: "" }, { host: "" }].forEach(redshift => {
      it("redshift_properties_check", () => {
        expect(() =>
          apiUtils.coerceCredentials("redshift", JSON.parse(JSON.stringify(redshift)))
        ).to.throw();

        expect(() =>
          apiUtils.coerceCredentials(
            "redshift",
            JSON.parse(JSON.stringify({ ...redshiftCredentials, oneMoreProperty: "" }))
          )
        ).to.not.throw(/Missing required properties/);
      });
    });

    [{}, { wrongProperty: "" }, { accountId: "" }].forEach(snowflake => {
      it("snowflake_properties_check", () => {
        expect(() =>
          apiUtils.coerceCredentials("snowflake", JSON.parse(JSON.stringify(snowflake)))
        ).to.throw();

        expect(() =>
          apiUtils.coerceCredentials(
            "snowflake",
            JSON.parse(JSON.stringify({ ...snowflakeCredentials, oneMoreProperty: "" }))
          )
        ).to.not.throw(/Missing required properties/);
      });
    });
  });

  describe("run", () => {
    const TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "foo",
        assertionSchema: "bar"
      },
      runConfig: {
        fullRefresh: true
      },
      warehouseState: {
        tables: [{ type: "table" }]
      },
      nodes: [
        {
          name: "node1",
          dependencies: [],
          tasks: [
            {
              type: "executionTaskType",
              statement: "SELECT foo FROM bar"
            }
          ],
          type: "table",
          target: {
            schema: "schema1",
            name: "target1"
          },
          tableType: "someTableType"
        },
        {
          name: "node2",
          dependencies: ["node1"],
          tasks: [
            {
              type: "executionTaskType2",
              statement: "SELECT bar FROM baz"
            }
          ],
          type: "assertion",
          target: {
            schema: "schema1",
            name: "target1"
          },
          tableType: "someTableType"
        }
      ]
    });

    it("execute", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      when(mockedDbAdapter.prepareSchema(anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(TEST_GRAPH.nodes[0].tasks[0].statement, anyFunction())
      ).thenResolve([]);
      when(
        mockedDbAdapter.execute(TEST_GRAPH.nodes[1].tasks[0].statement, anyFunction())
      ).thenReject(new Error("bad statement"));

      const runner = new Runner(instance(mockedDbAdapter), TEST_GRAPH);
      await runner.execute();
      const result = await runner.resultPromise();

      const timeCleanedNodes = result.nodes.map(node => {
        delete node.executionTime;
        return node;
      });
      result.nodes = timeCleanedNodes;

      expect(dataform.ExecutedGraph.create(result)).to.deep.equal(
        dataform.ExecutedGraph.create({
          projectConfig: TEST_GRAPH.projectConfig,
          runConfig: TEST_GRAPH.runConfig,
          warehouseState: TEST_GRAPH.warehouseState,
          ok: false,
          nodes: [
            {
              name: TEST_GRAPH.nodes[0].name,
              tasks: [
                {
                  task: TEST_GRAPH.nodes[0].tasks[0],
                  ok: true
                }
              ],
              status: dataform.NodeExecutionStatus.SUCCESSFUL,
              deprecatedOk: true
            },
            {
              name: TEST_GRAPH.nodes[1].name,
              tasks: [
                {
                  task: TEST_GRAPH.nodes[1].tasks[0],
                  ok: false,
                  error: "bad statement"
                }
              ],
              status: dataform.NodeExecutionStatus.FAILED,
              deprecatedOk: false
            }
          ]
        })
      );
    });

    it("execute_with_cancel", async () => {
      const TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
        projectConfig: {
          warehouse: "bigquery",
          defaultSchema: "foo",
          assertionSchema: "bar"
        },
        warehouseState: {
          tables: []
        },
        nodes: [
          {
            name: "node1",
            dependencies: [],
            tasks: [
              {
                type: "statement",
                statement: "some statement"
              }
            ],
            type: "table",
            target: {
              schema: "schema1",
              name: "target1"
            },
            tableType: "table"
          }
        ]
      });

      let wasCancelled = false;
      const mockDbAdapter = {
        execute: (_, onCancel) =>
          new Promise((_, reject) => {
            onCancel(() => {
              wasCancelled = true;
              reject(new Error("Run cancelled"));
            });
          }),
        prepareSchema: _ => {
          return Promise.resolve();
        }
      } as DbAdapter;

      const runner = new Runner(mockDbAdapter, TEST_GRAPH);
      const execution = runner.execute();
      // We want to await the return promise before we actually call cancel.
      // Setting a short (10ms) timeout on calling cancel accomplishes this.
      setTimeout(() => runner.cancel(), 10);
      const result = await execution;
      expect(wasCancelled).is.true;
      // Cancelling a run doesn't actually throw at the top level.
      // The node should fail, and have an appropriate error message.
      expect(result.nodes[0].deprecatedOk).is.false;
      expect(result.nodes[0].tasks[0].error).to.match(/cancelled/);
    });
  });
});
