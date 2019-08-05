import { Builder, compile, credentials, query, Runner } from "@dataform/api";
import { IDbAdapter } from "@dataform/api/dbadapters";
import { BigQueryDbAdapter } from "@dataform/api/dbadapters/bigquery";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { fail } from "assert";
import { assert, config, expect } from "chai";
import { asPlainObject, cleanSql } from "df/tests/utils";
import * as path from "path";
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
        { actions: ["a"], includeDependencies: true },
        TEST_STATE
      );
      const executionGraph = builder.build();
      const includedActionNames = executionGraph.actions.map(n => n.name);
      expect(includedActionNames).includes("a");
      expect(includedActionNames).includes("b");
    });

    it("exclude_deps", () => {
      const builder = new Builder(
        TEST_GRAPH,
        { actions: ["a"], includeDependencies: false },
        TEST_STATE
      );
      const executionGraph = builder.build();
      const includedActionNames = executionGraph.actions.map(n => n.name);
      expect(includedActionNames).includes("a");
      expect(includedActionNames).not.includes("b");
    });

    it("exclude_disabled", () => {
      const builder = new Builder(TEST_GRAPH, { includeDependencies: true }, TEST_STATE);
      const executionGraph = builder.build();

      const actionA = executionGraph.actions.find(n => n.name === "a");
      const actionB = executionGraph.actions.find(n => n.name === "b");
      const actionC = executionGraph.actions.find(n => n.name === "c");

      assert.exists(actionA);
      assert.exists(actionB);
      assert.exists(actionC);

      expect(actionA)
        .to.have.property("tasks")
        .to.be.an("array").that.not.is.empty;
      expect(actionB)
        .to.have.property("tasks")
        .to.be.an("array").that.is.empty;
      expect(actionC)
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

    it("trying to fully refresh a protected dataset fails", () => {
      const testGraph = dataform.CompiledGraph.create(TEST_GRAPH);
      testGraph.tables[0].protected = true;
      const builder = new Builder(TEST_GRAPH, { fullRefresh: true }, TEST_STATE);
      expect(() => builder.build()).to.throw();
    });

    it("action_types", () => {
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
        .to.have.property("actions")
        .to.be.an("array").that.is.not.empty;

      graph.tables.forEach(t => {
        const action = executedGraph.actions.find(item => item.name == t.name);
        expect(action).to.include({ type: "table", target: t.target, tableType: t.type });
      });

      graph.operations.forEach(t => {
        const action = executedGraph.actions.find(item => item.name == t.name);
        expect(action).to.include({ type: "operation", target: t.target });
      });

      graph.assertions.forEach(t => {
        const action = executedGraph.actions.find(item => item.name == t.name);
        expect(action).to.include({ type: "assertion" });
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
        .to.have.property("actions")
        .to.be.an("array").that.is.not.empty;

      const actionNames = executedGraph.actions.map(t => t.name);

      expect(actionNames).includes("a");
      expect(actionNames).not.includes("b");
      expect(actionNames).includes("c");
    });

    const TEST_GRAPH_WITH_TAGS: dataform.ICompiledGraph = dataform.CompiledGraph.create({
      projectConfig: { warehouse: "bigquery" },
      operations: [
        {
          name: "op_a",
          tags: ["tag1"],
          queries: ["create or replace view schema.someview as select 1 as test"]
        },
        {
          name: "op_b",
          dependencies: ["op_a"],
          tags: ["tag2"],
          queries: ["create or replace view schema.someview as select 1 as test"]
        },
        {
          name: "op_c",
          dependencies: ["op_a"],
          tags: ["tag3"],
          queries: ["create or replace view schema.someview as select 1 as test"]
        },
        {
          name: "op_d",
          tags: ["tag3"],
          queries: ["create or replace view schema.someview as select 1 as test"]
        }
      ],
      tables: [
        {
          name: "tab_a",
          dependencies: ["op_d"],
          target: {
            schema: "schema",
            name: "a"
          },
          tags: ["tag1", "tag2"]
        }
      ]
    });
    it("build actions with --tags (with dependencies)", () => {
      const builder = new Builder(
        TEST_GRAPH_WITH_TAGS,
        {
          actions: ["op_b", "op_d"],
          tags: ["tag1", "tag2", "tag4"],
          includeDependencies: true
        },
        TEST_STATE
      );
      const executedGraph = builder.build();
      const actionNames = executedGraph.actions.map(n => n.name);
      expect(actionNames).includes("op_a");
      expect(actionNames).includes("op_b");
      expect(actionNames).not.includes("op_c");
      expect(actionNames).includes("op_d");
      expect(actionNames).includes("tab_a");
    });

    it("build actions with --tags but without --actions (without dependencies)", () => {
      const builder = new Builder(
        TEST_GRAPH_WITH_TAGS,
        {
          tags: ["tag1", "tag2", "tag4"],
          includeDependencies: false
        },
        TEST_STATE
      );
      const executedGraph = builder.build();
      const actionNames = executedGraph.actions.map(n => n.name);
      expect(actionNames).includes("op_a");
      expect(actionNames).includes("op_b");
      expect(actionNames).not.includes("op_c");
      expect(actionNames).not.includes("op_d");
      expect(actionNames).includes("tab_a");
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
      expect(executionGraph.actions.filter(n => n.name == "incremental")).is.not.empty;
      expect(
        cleanSql(executionGraph.actions.filter(n => n.name == "incremental")[0].tasks[0].statement)
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

      expect(executionGraph.actions)
        .to.be.an("array")
        .to.have.lengthOf(4);

      executionGraph.actions.forEach((action, index) => {
        expect(action)
          .to.have.property("tasks")
          .to.be.an("array").that.is.not.empty;

        const statements = action.tasks.map(item => item.statement);
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
      const expectedExecutionActions: dataform.IExecutionAction[] = [
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
      expect(asPlainObject(executionGraph.actions)).deep.equals(
        asPlainObject(expectedExecutionActions)
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

      expect(executionGraph.actions)
        .to.be.an("array")
        .to.have.lengthOf(2);

      executionGraph.actions.forEach((action, index) => {
        expect(action)
          .to.have.property("tasks")
          .to.be.an("array").that.is.not.empty;

        const statements = action.tasks.map(item => item.statement);
        expect(statements).includes(
          `create or replace table "schema"."${action.name}" as select 1 as test`
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

      expect(graph.graphErrors).to.eql(dataform.GraphErrors.create());

      // Check JS blocks get processed.
      expect(tableNames).includes("df_integration_test.example_js_blocks");
      const exampleJsBlocks = graph.tables.filter(
        t => t.name == "df_integration_test.example_js_blocks"
      )[0];
      expect(exampleJsBlocks.type).equals("table");
      expect(exampleJsBlocks.query).equals("select 1 as foo");

      // Check we can import and use an external package.
      expect(tableNames).includes("df_integration_test.example_incremental");
      const exampleIncremental = graph.tables.filter(
        t => t.name == "df_integration_test.example_incremental"
      )[0];
      expect(exampleIncremental.query).equals("select current_timestamp() as ts");
      expect(exampleIncremental.where.trim()).equals(
        "ts > (select max(ts) from `tada-analytics.df_integration_test.example_incremental`) or (select max(ts) from `tada-analytics.df_integration_test.example_incremental`) is null"
      );

      // Check tables defined in includes are not included.
      expect(tableNames).not.includes("example_ignore");

      // Check SQL files with raw back-ticks get escaped.
      expect(tableNames).includes("df_integration_test.example_backticks");
      const exampleBackticks = graph.tables.filter(
        t => t.name == "df_integration_test.example_backticks"
      )[0];
      expect(cleanSql(exampleBackticks.query)).equals(
        "select * from `tada-analytics.df_integration_test.sample_data`"
      );

      // Check deferred calls to table resolve to the correct definitions file.
      expect(tableNames).includes("df_integration_test.example_deferred");
      const exampleDeferred = graph.tables.filter(
        t => t.name == "df_integration_test.example_deferred"
      )[0];
      expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");

      // Check inline tables
      expect(tableNames).includes("df_integration_test.example_inline");
      const exampleInline = graph.tables.filter(
        t => t.name == "df_integration_test.example_inline"
      )[0];
      expect(exampleInline.type).equals("inline");
      expect(exampleInline.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleInline.dependencies).includes("sample_data");

      expect(tableNames).includes("df_integration_test.example_using_inline");
      const exampleUsingInline = graph.tables.filter(
        t => t.name == "df_integration_test.example_using_inline"
      )[0];
      expect(exampleUsingInline.type).equals("table");
      expect(exampleUsingInline.query).equals(
        "\nselect * from (\nselect * from `tada-analytics.df_integration_test.sample_data`)\nwhere true"
      );
      expect(exampleUsingInline.dependencies).includes("sample_data");

      // Check view
      expect(tableNames).includes("df_integration_test.example_view");
      const exampleView = graph.tables.filter(t => t.name == "df_integration_test.example_view")[0];
      expect(exampleView.type).equals("view");
      expect(exampleView.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleView.dependencies).deep.equals(["sample_data"]);

      // Check table
      expect(tableNames).includes("df_integration_test.example_table");
      const exampleTable = graph.tables.filter(
        t => t.name == "df_integration_test.example_table"
      )[0];
      expect(exampleTable.type).equals("table");
      expect(exampleTable.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleTable.dependencies).deep.equals(["sample_data"]);

      // Check sample data
      expect(tableNames).includes("df_integration_test.sample_data");
      const exampleSampleData = graph.tables.filter(
        t => t.name == "df_integration_test.sample_data"
      )[0];
      expect(exampleSampleData.type).equals("view");
      expect(exampleSampleData.query).equals(
        "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
      );
      expect(exampleSampleData.dependencies).to.eql([]);
    });

    for (const schemaSuffix of ["", "suffix"]) {
      const schemaWithSuffix = (schema: string) =>
        schemaSuffix ? `${schema}_${schemaSuffix}` : schema;

      it(`bigquery using v2 language compiles with suffix "${schemaSuffix}"`, async () => {
        const graph = await compile({
          projectDir: path.resolve("df/examples/bigquery_language_v2"),
          schemaSuffixOverride: schemaSuffix
        });

        expect(graph.graphErrors).to.eql(
          dataform.GraphErrors.create({
            compilationErrors: [
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_bigquery.sqlx",
                message: "Actions may only specify 'bigquery: { ... }' if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_output.sqlx",
                message:
                  "Actions may only specify 'hasOutput: true' if they are of type 'operations' or create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_postops.sqlx",
                message: "Actions may only include post_operations if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_preops.sqlx",
                message: "Actions may only include pre_operations if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_redshift.sqlx",
                message: "Actions may only specify 'redshift: { ... }' if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/disabled_assertion.sqlx",
                message: "Actions may only specify 'disabled: true' if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/protected_assertion.sqlx",
                message:
                  "Actions may only specify 'protected: true' if they are of type 'incremental'."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/view_with_incremental.sqlx",
                message:
                  "Actions may only include incremental_where if they are of type 'incremental'."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/view_with_multiple_statements.sqlx",
                message:
                  "Actions may only contain more than one SQL statement if they are of type 'operations'."
              })
            ]
          })
        );

        // Check JS blocks get processed.
        const exampleJsBlocks = graph.tables.find(
          t => t.name === "df_integration_test.example_js_blocks"
        );
        expect(exampleJsBlocks).to.not.be.undefined;
        expect(exampleJsBlocks.type).equals("table");
        expect(exampleJsBlocks.query.trim()).equals("select 1 as foo");

        // Check we can import and use an external package.
        const exampleIncremental = graph.tables.find(
          t => t.name === "df_integration_test.example_incremental"
        );
        expect(exampleIncremental).to.not.be.undefined;
        expect(exampleIncremental.query.trim()).equals("select current_timestamp() as ts");
        expect(exampleIncremental.where.trim()).equals(
          `ts > (select max(ts) from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_incremental\`) or (select max(ts) from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_incremental\`) is null`
        );

        // Check tables defined in includes are not included.
        const exampleIgnore = graph.tables.find(
          t => t.name === "df_integration_test.example_ignore"
        );
        expect(exampleIgnore).to.be.undefined;

        // Check SQL files with raw back-ticks get escaped.
        const exampleBackticks = graph.tables.find(
          t => t.name === "df_integration_test.example_backticks"
        );
        expect(exampleBackticks).to.not.be.undefined;
        expect(cleanSql(exampleBackticks.query)).equals(
          "select * from `tada-analytics.df_integration_test.sample_data`"
        );
        expect(exampleBackticks.preOps).to.eql([
          '\n    GRANT SELECT ON `tada-analytics.df_integration_test.sample_data` TO GROUP "allusers@dataform.co"\n'
        ]);
        expect(exampleBackticks.postOps).to.eql([]);

        // Check deferred calls to table resolve to the correct definitions file.
        const exampleDeferred = graph.tables.find(
          t => t.name === "df_integration_test.example_deferred"
        );
        expect(exampleDeferred).to.not.be.undefined;
        expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");
        // Check inline tables
        const exampleInline = graph.tables.find(
          t => t.name === "df_integration_test.example_inline"
        );
        expect(exampleInline).to.not.be.undefined;
        expect(exampleInline.type).equals("inline");
        expect(exampleInline.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix("df_integration_test")}.sample_data\``
        );
        expect(exampleInline.dependencies).includes("sample_data");

        const exampleUsingInline = graph.tables.find(
          t => t.name === "df_integration_test.example_using_inline"
        );
        expect(exampleUsingInline).to.not.be.undefined;
        expect(exampleUsingInline.type).equals("table");
        expect(exampleUsingInline.query.trim()).equals(
          `select * from (\n\nselect * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\`\n)\nwhere true`
        );
        expect(exampleUsingInline.dependencies).includes("sample_data");

        // Check view
        const exampleView = graph.tables.find(t => t.name === "df_integration_test.example_view");
        expect(exampleView).to.not.be.undefined;
        expect(exampleView.type).equals("view");
        expect(exampleView.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix("df_integration_test")}.sample_data\``
        );
        expect(exampleView.dependencies).deep.equals(["sample_data"]);
        expect(exampleView.tags).to.eql([]);

        // Check table
        const exampleTable = graph.tables.find(t => t.name === "df_integration_test.example_table");
        expect(exampleTable).to.not.be.undefined;
        expect(exampleTable.type).equals("table");
        expect(exampleTable.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\`\n\n-- here \${"is"} a \`comment\n\n/* \${"another"} \` backtick \` containing \`\`\`comment */`
        );
        expect(exampleTable.dependencies).deep.equals(["sample_data"]);
        expect(exampleTable.preOps).to.eql([]);
        expect(exampleTable.postOps).to.eql([
          `\n    GRANT SELECT ON \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_table\` TO GROUP "allusers@dataform.co"\n`,
          `\n    GRANT SELECT ON \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_table\` TO GROUP "otherusers@dataform.co"\n`
        ]);
        expect(exampleTable.tags).to.eql([]);

        // Check Table with tags
        const exampleTableWithTags = graph.tables.find(
          t => t.name === "df_integration_test.example_table_with_tags"
        );
        expect(exampleTableWithTags).to.not.be.undefined;
        expect(exampleTableWithTags.tags).to.eql(["tag1", "tag2", "tag3"]);

        // Check sample data
        const exampleSampleData = graph.tables.find(
          t => t.name === "df_integration_test.sample_data"
        );
        expect(exampleSampleData).to.not.be.undefined;
        expect(exampleSampleData.type).equals("view");
        expect(exampleSampleData.query.trim()).equals(
          "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
        );
        expect(exampleSampleData.dependencies).to.eql([]);

        // Check schema overrides defined in "config {}"
        const exampleUsingOverriddenSchema = graph.tables.find(
          t => t.name === "override_schema.override_schema_example"
        );
        expect(exampleUsingOverriddenSchema).to.not.be.undefined;
        expect(exampleUsingOverriddenSchema.target.schema).equals(
          schemaWithSuffix("override_schema")
        );
        expect(exampleUsingOverriddenSchema.type).equals("view");
        expect(exampleUsingOverriddenSchema.query.trim()).equals(
          "select 1 as test_schema_override"
        );

        // Check assertion
        const exampleAssertion = graph.assertions.find(
          t => t.name === "hi_there.example_assertion"
        );
        expect(exampleAssertion).to.not.be.undefined;
        expect(exampleAssertion.target.schema).equals(schemaWithSuffix("hi_there"));
        expect(exampleAssertion.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\` where sample = 100`
        );
        expect(exampleAssertion.dependencies).to.eql(["sample_data"]);
        expect(exampleAssertion.tags).to.eql([]);

        // Check Assertion with tags
        const exampleAssertionWithTags = graph.assertions.find(
          t => t.name === "hi_there.example_assertion_with_tags"
        );
        expect(exampleAssertionWithTags).to.not.be.undefined;
        expect(exampleAssertionWithTags.target.schema).equals(
          schemaWithSuffix("df_integration_test_assertions")
        );
        expect(exampleAssertionWithTags.tags).to.eql(["tag1", "tag2"]);

        // Check example operations file
        const exampleOperations = graph.operations.find(
          o => o.name === "df_integration_test.example_operations"
        );
        expect(exampleOperations).to.not.be.undefined;
        expect(exampleOperations.target).is.null;
        expect(exampleOperations.queries).to.eql([
          "\n\nCREATE OR REPLACE VIEW someschema.someview AS (SELECT 1 AS test)\n",
          `\nDROP VIEW IF EXISTS \`tada-analytics.${schemaWithSuffix(
            "override_schema"
          )}.override_schema_example\`\n`
        ]);
        expect(exampleOperations.dependencies).to.eql([
          "example_inline",
          "override_schema_example"
        ]);
        expect(exampleOperations.tags).to.eql([]);

        // Check example operation with output.
        const exampleOperationWithOutput = graph.operations.find(
          o => o.name === "df_integration_test.example_operation_with_output"
        );
        expect(exampleOperationWithOutput).to.not.be.undefined;
        expect(exampleOperationWithOutput.target.schema).equals(
          schemaWithSuffix("df_integration_test")
        );
        expect(exampleOperationWithOutput.target.name).equals(
          "df_integration_test.example_operation_with_output"
        );
        expect(exampleOperationWithOutput.queries).to.eql([
          `\nCREATE OR REPLACE VIEW \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_operation_with_output\` AS (SELECT 1 AS TEST)`
        ]);
        expect(exampleOperationWithOutput.dependencies).to.eql([]);

        // Check Operation with tags
        const exampleOperationsWithTags = graph.operations.find(
          t => t.name === "df_integration_test.example_operations_with_tags"
        );
        expect(exampleOperationsWithTags).to.not.be.undefined;
        expect(exampleOperationsWithTags.tags).to.eql(["tag1"]);

        // Check testcase.
        const testCase = graph.tests.find(t => t.name === "example_test_case");
        expect(testCase.testQuery.trim()).equals(
          "select * from (\n    select 'hi' as faked union all\n    select 'ben' as faked union all\n    select 'sup?' as faked\n)\n\n-- here ${\"is\"} a `comment\n\n/* ${\"another\"} ` backtick ` containing ```comment */"
        );
        expect(testCase.expectedOutputQuery.trim()).equals(
          "select 'hi' as faked union all\nselect 'ben' as faked union all\nselect 'sup?' as faked"
        );
      });
    }

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
        expect(tableNames).includes("df_integration_test.example_incremental");
        const exampleIncremental = graph.tables.filter(
          t => t.name == "df_integration_test.example_incremental"
        )[0];
        expect(exampleIncremental.query).equals("select current_timestamp::timestamp as ts");

        // Check inline tables
        expect(tableNames).includes("df_integration_test.example_inline");
        const exampleInline = graph.tables.filter(
          t => t.name == "df_integration_test.example_inline"
        )[0];
        expect(exampleInline.type).equals("inline");
        expect(exampleInline.query).equals('\nselect * from "df_integration_test"."sample_data"');
        expect(exampleInline.dependencies).includes("sample_data");

        expect(tableNames).includes("df_integration_test.example_using_inline");
        const exampleUsingInline = graph.tables.filter(
          t => t.name == "df_integration_test.example_using_inline"
        )[0];
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

      expect(graph.graphErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array");

      expectedResults.forEach(result => {
        const error = graph.graphErrors.compilationErrors.find(item =>
          result.message.test(item.message)
        );

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
        "test_schema.sample_1": 'create table "test_schema"."sample_1" as select 1 as sample_1',
        "test_schema.sample_2": 'select * from "test_schema"."sample_1"'
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

      expect(mNames).includes("df_integration_test.example_incremental");
      const mIncremental = graph.tables.filter(
        t => t.name == "df_integration_test.example_incremental"
      )[0];
      expect(mIncremental.type).equals("incremental");
      expect(mIncremental.query).equals(
        "select convert_timezone('UTC', current_timestamp())::timestamp as ts"
      );
      expect(mIncremental.dependencies).to.be.an("array").that.is.empty;

      expect(mNames).includes("df_integration_test.example_table");
      const mTable = graph.tables.filter(t => t.name == "df_integration_test.example_table")[0];
      expect(mTable.type).equals("table");
      expect(mTable.query).equals('\nselect * from "df_integration_test"."sample_data"');
      expect(mTable.dependencies).deep.equals(["sample_data"]);

      expect(mNames).includes("df_integration_test.example_view");
      const mView = graph.tables.filter(t => t.name == "df_integration_test.example_view")[0];
      expect(mView.type).equals("view");
      expect(mView.query).equals('\nselect * from "df_integration_test"."sample_data"');
      expect(mView.dependencies).deep.equals(["sample_data"]);

      expect(mNames).includes("df_integration_test.sample_data");
      const mSampleData = graph.tables.filter(t => t.name == "df_integration_test.sample_data")[0];
      expect(mSampleData.type).equals("view");
      expect(mSampleData.query).equals(
        "select 1 as sample_column union all\nselect 2 as sample_column union all\nselect 3 as sample_column"
      );
      expect(mSampleData.dependencies).to.be.an("array").that.is.empty;

      // Check inline tables
      expect(mNames).includes("df_integration_test.example_inline");
      const exampleInline = graph.tables.filter(
        t => t.name == "df_integration_test.example_inline"
      )[0];
      expect(exampleInline.type).equals("inline");
      expect(exampleInline.query).equals('\nselect * from "df_integration_test"."sample_data"');
      expect(exampleInline.dependencies).includes("sample_data");

      expect(mNames).includes("df_integration_test.example_using_inline");
      const exampleUsingInline = graph.tables.filter(
        t => t.name == "df_integration_test.example_using_inline"
      )[0];
      expect(exampleUsingInline.type).equals("table");
      expect(exampleUsingInline.query).equals(
        '\nselect * from (\nselect * from "df_integration_test"."sample_data")\nwhere true'
      );
      expect(exampleUsingInline.dependencies).includes("sample_data");

      const aNames = graph.assertions.map(a => a.name);

      expect(aNames).includes("df_integration_test_assertions.sample_data_assertion");
      const assertion = graph.assertions.filter(
        a => a.name == "df_integration_test_assertions.sample_data_assertion"
      )[0];
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

    it("times out after timeout period during compilation", async () => {
      try {
        await compile({ projectDir: "df/examples/redshift_never_finishes_compiling" });
        fail("Compilation timeout Error expected.");
      } catch (e) {
        expect(e.message).to.equal("Compilation timed out");
      }
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
        expect(() => credentials.coerce(warehouse, null)).to.throw(
          /Credentials JSON object does not conform to protobuf requirements: object expected/
        );
        expect(() => credentials.coerce(warehouse, {})).to.throw(/Missing required properties:/);
      });
    });

    it("warehouse_check", () => {
      expect(() => credentials.coerce("bigquery", bigqueryCredentials)).to.not.throw();
      expect(() => credentials.coerce("redshift", redshiftCredentials)).to.not.throw();
      expect(() => credentials.coerce("snowflake", snowflakeCredentials)).to.not.throw();
      expect(() => credentials.coerce("some_other_warehouse", {})).to.throw(
        /Unrecognized warehouse:/
      );
    });

    [{}, { wrongProperty: "" }, { projectId: "" }].forEach(bigquery => {
      it("bigquery_properties_check", () => {
        expect(() =>
          credentials.coerce("bigquery", JSON.parse(JSON.stringify(bigquery)))
        ).to.throw();

        expect(() =>
          credentials.coerce(
            "bigquery",
            JSON.parse(JSON.stringify({ ...bigqueryCredentials, oneMoreProperty: "" }))
          )
        ).to.not.throw(/Missing required properties/);
      });
    });

    [{}, { wrongProperty: "" }, { host: "" }].forEach(redshift => {
      it("redshift_properties_check", () => {
        expect(() =>
          credentials.coerce("redshift", JSON.parse(JSON.stringify(redshift)))
        ).to.throw();

        expect(() =>
          credentials.coerce(
            "redshift",
            JSON.parse(JSON.stringify({ ...redshiftCredentials, oneMoreProperty: "" }))
          )
        ).to.not.throw(/Missing required properties/);
      });
    });

    [{}, { wrongProperty: "" }, { accountId: "" }].forEach(snowflake => {
      it("snowflake_properties_check", () => {
        expect(() =>
          credentials.coerce("snowflake", JSON.parse(JSON.stringify(snowflake)))
        ).to.throw();

        expect(() =>
          credentials.coerce(
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
      actions: [
        {
          name: "action1",
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
          name: "action2",
          dependencies: ["action1"],
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
        mockedDbAdapter.execute(TEST_GRAPH.actions[0].tasks[0].statement, anyFunction())
      ).thenResolve([]);
      when(
        mockedDbAdapter.execute(TEST_GRAPH.actions[1].tasks[0].statement, anyFunction())
      ).thenReject(new Error("bad statement"));

      const runner = new Runner(instance(mockedDbAdapter), TEST_GRAPH);
      await runner.execute();
      const result = await runner.resultPromise();

      const timeCleanedActions = result.actions.map(action => {
        delete action.executionTime;
        return action;
      });
      result.actions = timeCleanedActions;

      expect(dataform.ExecutedGraph.create(result)).to.deep.equal(
        dataform.ExecutedGraph.create({
          projectConfig: TEST_GRAPH.projectConfig,
          runConfig: TEST_GRAPH.runConfig,
          warehouseState: TEST_GRAPH.warehouseState,
          ok: false,
          actions: [
            {
              name: TEST_GRAPH.actions[0].name,
              tasks: [
                {
                  task: TEST_GRAPH.actions[0].tasks[0],
                  ok: true
                }
              ],
              status: dataform.ActionExecutionStatus.SUCCESSFUL,
              deprecatedOk: true
            },
            {
              name: TEST_GRAPH.actions[1].name,
              tasks: [
                {
                  task: TEST_GRAPH.actions[1].tasks[0],
                  ok: false,
                  error: "bad statement"
                }
              ],
              status: dataform.ActionExecutionStatus.FAILED,
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
        actions: [
          {
            name: "action1",
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
      } as IDbAdapter;

      const runner = new Runner(mockDbAdapter, TEST_GRAPH);
      const execution = runner.execute();
      // We want to await the return promise before we actually call cancel.
      // Setting a short (10ms) timeout on calling cancel accomplishes this.
      setTimeout(() => runner.cancel(), 10);
      const result = await execution;
      expect(wasCancelled).is.true;
      // Cancelling a run doesn't actually throw at the top level.
      // The action should fail, and have an appropriate error message.
      expect(result.actions[0].deprecatedOk).is.false;
      expect(result.actions[0].tasks[0].error).to.match(/cancelled/);
    });
  });
});
