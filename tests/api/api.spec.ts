import { assert, config, expect } from "chai";
import Long from "long";
import { anyString, anything, instance, mock, verify, when } from "ts-mockito";

import { Builder, credentials, prune, query, Runner } from "df/api";
import { computeAllTransitiveInputs } from "df/api/commands/build";
import { IDbAdapter } from "df/api/dbadapters";
import { BigQueryDbAdapter } from "df/api/dbadapters/bigquery";
import { parseSnowflakeEvalError } from "df/api/utils/error_parsing";
import { sleep, sleepUntil } from "df/common/promises";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { asPlainObject, cleanSql } from "df/tests/utils";

config.truncateThreshold = 0;

suite("@dataform/api", () => {
  const TEST_GRAPH: dataform.ICompiledGraph = dataform.CompiledGraph.create({
    projectConfig: { warehouse: "redshift" },
    tables: [
      {
        name: "schema.a",
        type: "table",
        target: {
          schema: "schema",
          name: "a"
        },
        query: "query",
        dependencies: ["schema.b"]
      },
      {
        name: "schema.b",
        type: "table",
        target: {
          schema: "schema",
          name: "b"
        },
        query: "query",
        dependencies: ["schema.c"],
        disabled: true
      },
      {
        name: "schema.c",
        type: "table",
        target: {
          schema: "schema",
          name: "c"
        },
        query: "query"
      }
    ],
    assertions: [
      {
        name: "schema.d",
        target: {
          schema: "schema",
          name: "d"
        },
        parentAction: {
          schema: "schema",
          name: "b"
        }
      }
    ]
  });

  const TEST_STATE = dataform.WarehouseState.create({ tables: [] });

  suite("build", () => {
    test("exclude_disabled", () => {
      const builder = new Builder(
        TEST_GRAPH,
        { includeDependencies: true },
        TEST_STATE,
        computeAllTransitiveInputs(TEST_GRAPH)
      );
      const executionGraph = builder.build();

      const actionA = executionGraph.actions.find(n => n.name === "schema.a");
      const actionB = executionGraph.actions.find(n => n.name === "schema.b");
      const actionC = executionGraph.actions.find(n => n.name === "schema.c");

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
          projectConfig: { warehouse: "redshift" },
          graphErrors: { compilationErrors: [{ message: "Some critical error" }] },
          tables: [{ name: "a", target: { schema: "schema", name: "a" } }]
        });

        const builder = new Builder(
          graphWithErrors,
          {},
          TEST_STATE,
          computeAllTransitiveInputs(graphWithErrors)
        );
        builder.build();
      }).to.throw();
    });

    test("trying to fully refresh a protected dataset fails", () => {
      const testGraph = dataform.CompiledGraph.create(TEST_GRAPH);
      testGraph.tables[0].protected = true;
      const builder = new Builder(
        TEST_GRAPH,
        { fullRefresh: true },
        TEST_STATE,
        computeAllTransitiveInputs(TEST_GRAPH)
      );
      expect(() => builder.build()).to.throw();
    });

    test("action_types", () => {
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

      const builder = new Builder(graph, {}, TEST_STATE, computeAllTransitiveInputs(graph));
      const executedGraph = builder.build();

      expect(executedGraph.actions.length).greaterThan(0);

      graph.tables.forEach((t: dataform.ITable) => {
        const action = executedGraph.actions.find(item => item.name === t.name);
        expect(action).to.include({ type: "table", target: t.target, tableType: t.type });
      });

      graph.operations.forEach((o: dataform.IOperation) => {
        const action = executedGraph.actions.find(item => item.name === o.name);
        expect(action).to.include({ type: "operation", target: o.target });
      });

      graph.assertions.forEach((a: dataform.IAssertion) => {
        const action = executedGraph.actions.find(item => item.name === a.name);
        expect(action).to.include({ type: "assertion" });
      });
    });

    suite("pre and post ops", () => {
      for (const warehouse of [
        "bigquery",
        "postgres",
        "redshift",
        "sqldatawarehouse",
        "snowflake"
      ]) {
        const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
          projectConfig: { warehouse: "redshift" },
          tables: [
            {
              name: "a",
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

        test(`${warehouse} when running non incrementally`, () => {
          const action = new Builder(
            graph,
            {},
            TEST_STATE,
            computeAllTransitiveInputs(graph)
          ).build().actions[0];
          expect(action.tasks[0]).eql(
            dataform.ExecutionTask.create({
              type: "statement",
              statement: "preOp"
            })
          );
          expect(action.tasks.slice(-1)[0]).eql(
            dataform.ExecutionTask.create({
              type: "statement",
              statement: "postOp"
            })
          );
        });

        test(`${warehouse} when running incrementally`, () => {
          const action = new Builder(
            graph,
            {},
            dataform.WarehouseState.create({
              tables: [{ target: graph.tables[0].target, fields: [] }]
            }),
            computeAllTransitiveInputs(graph)
          ).build().actions[0];
          expect(action.tasks[0]).eql(
            dataform.ExecutionTask.create({
              type: "statement",
              statement: "incremental preOp"
            })
          );
          expect(action.tasks.slice(-1)[0]).eql(
            dataform.ExecutionTask.create({
              type: "statement",
              statement: "incremental postOp"
            })
          );
        });
      }
    });
  });

  suite("prune", () => {
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

    test("prune removes inline tables", async () => {
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

      const prunedGraph = prune(graph, {});

      expect(prunedGraph.tables.length).greaterThan(0);

      const actionNames = prunedGraph.tables.map(action => action.name);

      expect(actionNames).includes("a");
      expect(actionNames).not.includes("b");
      expect(actionNames).includes("c");
    });

    test("prune actions with --tags (with dependencies)", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        actions: ["op_b", "op_d"],
        tags: ["tag1", "tag2", "tag4"],
        includeDependencies: true
      });
      const actionNames = [
        ...prunedGraph.tables.map(action => action.name),
        ...prunedGraph.operations.map(action => action.name)
      ];
      expect(actionNames).includes("op_a");
      expect(actionNames).includes("op_b");
      expect(actionNames).not.includes("op_c");
      expect(actionNames).includes("op_d");
      expect(actionNames).includes("tab_a");
    });

    test("prune actions with --tags but without --actions (without dependencies)", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        tags: ["tag1", "tag2", "tag4"],
        includeDependencies: false
      });
      const actionNames = [
        ...prunedGraph.tables.map(action => action.name),
        ...prunedGraph.operations.map(action => action.name)
      ];
      expect(actionNames).includes("op_a");
      expect(actionNames).includes("op_b");
      expect(actionNames).not.includes("op_c");
      expect(actionNames).not.includes("op_d");
      expect(actionNames).includes("tab_a");
    });

    test("prune actions with --actions with dependencies", () => {
      const prunedGraph = prune(TEST_GRAPH, { actions: ["schema.a"], includeDependencies: true });
      const actionNames = [
        ...prunedGraph.tables.map(action => action.name),
        ...prunedGraph.operations.map(action => action.name),
        ...prunedGraph.assertions.map(action => action.name)
      ];
      expect(actionNames).includes("schema.a");
      expect(actionNames).includes("schema.b");
      expect(actionNames).includes("schema.d");
    });

    test("prune actions with --actions without dependencies", () => {
      const prunedGraph = prune(TEST_GRAPH, { actions: ["schema.a"], includeDependencies: false });
      const actionNames = [
        ...prunedGraph.tables.map(action => action.name),
        ...prunedGraph.operations.map(action => action.name),
        ...prunedGraph.assertions.map(action => action.name)
      ];
      expect(actionNames).includes("schema.a");
      expect(actionNames).not.includes("schema.b");
      expect(actionNames).not.includes("schema.d");
    });
  });

  suite("sql_generating", () => {
    test("bigquery_incremental", () => {
      const graph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb" },
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
            type: dataform.TableMetadata.Type.TABLE,
            fields: [
              {
                name: "existing_field"
              }
            ]
          }
        ]
      });
      const executionGraph = new Builder(
        graph,
        {},
        state,
        computeAllTransitiveInputs(graph)
      ).build();
      expect(
        cleanSql(executionGraph.actions.filter(n => n.name === "incremental")[0].tasks[0].statement)
      ).equals(
        cleanSql(
          `insert into \`deeb.schema.incremental\` (existing_field)
           select existing_field from (
             select * from (select 1 as test) as subquery
             where true
           ) as insertions`
        )
      );
    });

    test("redshift_create", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "redshift" },
        dataformCoreVersion: "1.4.1",
        tables: [
          {
            name: "redshift_all",
            type: "table",
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
            type: "table",
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
            type: "table",
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
            type: "table",
            target: {
              schema: "schema",
              name: "redshift_without_redshift"
            },
            query: "query"
          },
          {
            name: "redshift_view",
            type: "view",
            target: {
              schema: "schema",
              name: "redshift_view"
            },
            query: "query"
          },
          {
            name: "redshift_view_with_binding",
            type: "view",
            target: {
              schema: "schema",
              name: "redshift_view_with_binding"
            },
            query: "query",
            redshift: {
              bind: true
            }
          }
        ]
      });
      const testState = dataform.WarehouseState.create({});
      const expectedSQL = [
        'create table "schema"."redshift_all_temp" diststyle even distkey (column1) compound sortkey (column1, column2) as query',
        'create table "schema"."redshift_only_sort_temp" interleaved sortkey (column1) as query',
        'create table "schema"."redshift_only_dist_temp" diststyle even distkey (column1) as query',
        'create table "schema"."redshift_without_redshift_temp" as query',
        'create or replace view "schema"."redshift_view" as query with no schema binding',
        'create or replace view "schema"."redshift_view_with_binding" as query'
      ];

      const builder = new Builder(testGraph, {}, testState, computeAllTransitiveInputs(testGraph));
      const executionGraph = builder.build();

      expect(executionGraph.actions)
        .to.be.an("array")
        .to.have.lengthOf(6);

      executionGraph.actions.forEach((action, index) => {
        expect(action.tasks.length).greaterThan(0);

        const statements = action.tasks.map(item => item.statement);
        expect(statements).includes(expectedSQL[index]);
      });
    });

    test("redshift_create after bind support dropped", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "redshift" },
        dataformCoreVersion: "1.11.0",
        tables: [
          {
            name: "redshift_view_with_binding",
            type: "view",
            target: {
              schema: "schema",
              name: "redshift_view_with_binding"
            },
            query: "query",
            redshift: {
              bind: true
            }
          }
        ]
      });

      const builder = new Builder(
        testGraph,
        {},
        dataform.WarehouseState.create({}),
        computeAllTransitiveInputs(testGraph)
      );
      const executionGraph = builder.build();

      expect(
        executionGraph.actions.map(action => action.tasks.map(task => task.statement)).flat()
      ).eql([
        'drop view if exists "schema"."redshift_view_with_binding"',
        'create or replace view "schema"."redshift_view_with_binding" as query with no schema binding'
      ]);
    });

    test("postgres_create", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "postgres" },
        dataformCoreVersion: "1.4.1",
        tables: [
          {
            name: "postgres_view",
            type: "view",
            target: {
              schema: "schema",
              name: "postgres_view"
            },
            query: "query"
          }
        ]
      });
      const testState = dataform.WarehouseState.create({});
      const expectedSQL = ['create or replace view "schema"."postgres_view" as query'];

      const builder = new Builder(testGraph, {}, testState, computeAllTransitiveInputs(testGraph));
      const executionGraph = builder.build();

      expect(executionGraph.actions)
        .to.be.an("array")
        .to.have.lengthOf(1);

      executionGraph.actions.forEach((action, index) => {
        expect(action.tasks.length).greaterThan(0);

        const statements = action.tasks.map(item => item.statement);
        expect(statements).includes(expectedSQL[index]);
      });
    });

    test("bigquery_partitionby", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb" },
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
              partitionBy: "DATE(test)",
              clusterBy: []
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
                "create or replace table `deeb.schema.name` partition by DATE(test) as select 1 as test"
            }
          ],
          dependencies: [],
          transitiveInputs: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
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
              statement: "create or replace table `deeb.schema.name` as select 1 as test"
            }
          ],
          dependencies: [],
          transitiveInputs: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        }
      ];
      const executionGraph = new Builder(
        testGraph,
        {},
        dataform.WarehouseState.create({}),
        computeAllTransitiveInputs(testGraph)
      ).build();
      expect(asPlainObject(executionGraph.actions)).deep.equals(
        asPlainObject(expectedExecutionActions)
      );
    });

    test("bigquery_clusterby", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb" },
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
              partitionBy: "DATE(test)",
              clusterBy: ["name", "revenue"]
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
                "create or replace table `deeb.schema.name` partition by DATE(test) cluster by name, revenue as select 1 as test"
            }
          ],
          dependencies: [],
          transitiveInputs: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
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
              statement: "create or replace table `deeb.schema.name` as select 1 as test"
            }
          ],
          dependencies: [],
          transitiveInputs: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        }
      ];
      const executionGraph = new Builder(
        testGraph,
        {},
        dataform.WarehouseState.create({}),
        computeAllTransitiveInputs(testGraph)
      ).build();
      expect(asPlainObject(executionGraph.actions)).deep.equals(
        asPlainObject(expectedExecutionActions)
      );
    });

    test("snowflake", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "snowflake" },
        tables: [
          {
            name: "a",
            type: "table",
            target: {
              schema: "schema",
              name: "a"
            },
            query: "select 1 as test"
          },
          {
            name: "b",
            type: "table",
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
      const builder = new Builder(testGraph, {}, testState, computeAllTransitiveInputs(testGraph));
      const executionGraph = builder.build();

      expect(executionGraph.actions)
        .to.be.an("array")
        .to.have.lengthOf(2);

      executionGraph.actions.forEach((action, index) => {
        expect(action.tasks.length).greaterThan(0);

        const statements = action.tasks.map(item => item.statement);
        expect(statements).includes(
          `create or replace table "schema"."${action.name}" as select 1 as test`
        );
      });
    });

    [
      {
        warehouse: "bigquery",
        expectedQuery: "preOps;\ncreate or replace table `database.schema.b` as query;\npostOps"
      },
      {
        warehouse: "sqldatawarehouse",
        expectedQuery: `preOps;
if object_id ('"schema"."b_temp"','U') is not null drop table "schema"."b_temp";
create table "schema"."b_temp"
     with(
       distribution = ROUND_ROBIN
     ) 
     as query;
if object_id ('"schema"."b"','U') is not null drop table "schema"."b";
rename object "schema"."b_temp" to b;
postOps`
      }
    ].forEach(({ warehouse, expectedQuery }) => {
      test(`${warehouse}_useSingleQueryPerAction`, async () => {
        const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
          projectConfig: { warehouse, useSingleQueryPerAction: true },
          tables: [
            {
              name: "a",
              type: "table",
              query: "query",
              preOps: ["preOps"],
              postOps: ["postOps"],
              target: { schema: "schema", name: "b", database: "database" }
            }
          ]
        });
        const testState = dataform.WarehouseState.create({});
        const builder = new Builder(
          testGraph,
          { useSingleQueryPerAction: true },
          testState,
          computeAllTransitiveInputs(testGraph)
        );
        const executionGraph = builder.build();

        expect(executionGraph.actions)
          .to.be.an("array")
          .to.have.lengthOf(1);

        const tasks = executionGraph.actions[0].tasks;
        expect(tasks.length).to.equal(1);
        expect(tasks[0].statement).to.equal(expectedQuery);
      });
    });
  });

  suite("query", () => {
    test("bigquery_example", async () => {
      const compiledQuery = await query.compile('select 1 from ${ref("example_view")}', {
        projectDir: "examples/common_v1",
        projectConfigOverride: { warehouse: "bigquery", defaultDatabase: "tada-analytics" }
      });
      expect(compiledQuery).equals(
        "select 1 from `tada-analytics.df_integration_test.example_view`"
      );
    });
    test("bigquery example with input backticks", async () => {
      const compiledQuery = await query.compile(
        "select 1 from `tada-analytics.df_integration_test.example_view`",
        {
          projectDir: "examples/common_v1",
          projectConfigOverride: { warehouse: "bigquery", defaultDatabase: "tada-analytics" }
        }
      );
      expect(compiledQuery).equals(
        "select 1 from `tada-analytics.df_integration_test.example_view`"
      );
    });
    test("bigquery example with a backslash in regex", async () => {
      const compiledQuery = await query.compile(
        "select regexp_extract('01a_data_engine', '^(\\d{2}\\w)')",
        {
          projectDir: "examples/common_v1",
          projectConfigOverride: { warehouse: "bigquery", defaultDatabase: "tada-analytics" }
        }
      );
      expect(compiledQuery).equals("select regexp_extract('01a_data_engine', '^(\\d{2}\\w)')");
    });
  });

  suite("credentials_config", () => {
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
      test(`${warehouse}_empty_credentials`, () => {
        expect(() => credentials.coerce(warehouse, null)).to.throw(
          /Credentials JSON object does not conform to protobuf requirements: object expected/
        );
        expect(() => credentials.coerce(warehouse, {})).to.throw(/Missing required properties:/);
      });
    });

    test("warehouse_check", () => {
      expect(() => credentials.coerce("bigquery", bigqueryCredentials)).to.not.throw();
      expect(() => credentials.coerce("redshift", redshiftCredentials)).to.not.throw();
      expect(() => credentials.coerce("snowflake", snowflakeCredentials)).to.not.throw();
      expect(() => credentials.coerce("some_other_warehouse", {})).to.throw(
        /Unrecognized warehouse:/
      );
    });

    [{}, { wrongProperty: "" }, { projectId: "" }].forEach(bigquery => {
      test("bigquery_properties_check", () => {
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
      test("redshift_properties_check", () => {
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
      test("snowflake_properties_check", () => {
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

  suite("run", () => {
    const RUN_TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
      projectConfig: {
        warehouse: "bigquery",
        defaultSchema: "foo",
        assertionSchema: "bar",
        defaultDatabase: "database"
      },
      runConfig: {
        fullRefresh: true
      },
      warehouseState: {
        tables: [
          {
            type: dataform.TableMetadata.Type.TABLE,
            target: {
              schema: "schema1",
              name: "target1"
            }
          }
        ]
      },
      actions: [
        {
          name: "action1",
          dependencies: [],
          tasks: [
            {
              type: "executionTaskType",
              statement: "SELECT foo FROM bar"
            },
            {
              type: "executionTaskType",
              statement: "SELECT 42"
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
            database: "database2",
            schema: "schema2",
            name: "target2"
          },
          tableType: "someTableType"
        }
      ]
    });

    const EXPECTED_RUN_RESULT = dataform.RunResult.create({
      status: dataform.RunResult.ExecutionStatus.FAILED,
      actions: [
        {
          name: RUN_TEST_GRAPH.actions[0].name,
          target: RUN_TEST_GRAPH.actions[0].target,
          tasks: [
            {
              status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
              metadata: {
                bigquery: {
                  jobId: "abc",
                  totalBytesBilled: Long.fromNumber(0),
                  totalBytesProcessed: Long.fromNumber(0)
                }
              }
            },
            {
              status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
              metadata: {}
            }
          ],
          status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL
        },
        {
          name: RUN_TEST_GRAPH.actions[1].name,
          target: RUN_TEST_GRAPH.actions[1].target,
          tasks: [
            {
              status: dataform.TaskResult.ExecutionStatus.FAILED,
              metadata: {},
              errorMessage: "bigquery error: bad statement"
            }
          ],
          status: dataform.ActionResult.ExecutionStatus.FAILED
        }
      ]
    });

    test("execute", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[0].tasks[0].statement, anything())
      ).thenResolve({
        rows: [],
        metadata: {
          bigquery: {
            jobId: "abc",
            totalBytesBilled: Long.fromNumber(0),
            totalBytesProcessed: Long.fromNumber(0)
          }
        }
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[0].tasks[1].statement, anything())
      ).thenResolve({
        rows: [],
        metadata: {}
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[1].tasks[0].statement, anything())
      ).thenReject(new Error("bad statement"));

      const mockDbAdapterInstance = instance(mockedDbAdapter);
      mockDbAdapterInstance.withClientLock = async callback =>
        await callback(mockDbAdapterInstance);

      const runner = new Runner(mockDbAdapterInstance, RUN_TEST_GRAPH);

      expect(dataform.RunResult.create(cleanTiming(await runner.execute().result()))).to.deep.equal(
        EXPECTED_RUN_RESULT
      );
      verify(mockedDbAdapter.createSchema("database", "schema1")).once();
      verify(mockedDbAdapter.createSchema("database2", "schema2")).once();
    });

    test("stop and then resume", async () => {
      let firstQueryInProgress = false;
      let stopWasCalled = false;

      const mockedDbAdapter = mock(BigQueryDbAdapter);
      when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[0].tasks[0].statement, anything())
      ).thenCall(async () => {
        firstQueryInProgress = true;
        await sleepUntil(() => stopWasCalled);
        return {
          rows: [],
          metadata: {
            bigquery: {
              jobId: "abc",
              totalBytesBilled: Long.fromNumber(0),
              totalBytesProcessed: Long.fromNumber(0)
            }
          }
        };
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[0].tasks[1].statement, anything())
      ).thenResolve({
        rows: [],
        metadata: {}
      });
      when(
        mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[1].tasks[0].statement, anything())
      ).thenReject(new Error("bad statement"));

      const mockDbAdapterInstance = instance(mockedDbAdapter);
      mockDbAdapterInstance.withClientLock = async callback =>
        await callback(mockDbAdapterInstance);

      let runner = new Runner(mockDbAdapterInstance, RUN_TEST_GRAPH);
      runner.execute();
      await sleepUntil(() => firstQueryInProgress);
      runner.stop();
      stopWasCalled = true;
      const result = cleanTiming(await runner.result());

      expect(dataform.RunResult.create(result)).to.deep.equal(
        dataform.RunResult.create({
          status: dataform.RunResult.ExecutionStatus.RUNNING,
          actions: [
            {
              name: EXPECTED_RUN_RESULT.actions[0].name,
              target: EXPECTED_RUN_RESULT.actions[0].target,
              status: dataform.ActionResult.ExecutionStatus.RUNNING,
              tasks: [EXPECTED_RUN_RESULT.actions[0].tasks[0]]
            }
          ]
        })
      );

      runner = new Runner(mockDbAdapterInstance, RUN_TEST_GRAPH, result);

      expect(dataform.RunResult.create(cleanTiming(await runner.execute().result()))).to.deep.equal(
        EXPECTED_RUN_RESULT
      );
      verify(mockedDbAdapter.createSchema("database", "schema1")).once();
      verify(mockedDbAdapter.createSchema("database2", "schema2")).once();
    });

    suite("execute with retry", () => {
      test("should fail when execution fails too many times for the retry setting", async () => {
        const mockedDbAdapter = mock(BigQueryDbAdapter);
        const NEW_TEST_GRAPH = {
          ...RUN_TEST_GRAPH,
          projectConfig: { ...RUN_TEST_GRAPH.projectConfig, idempotentActionRetries: 1 }
        };
        when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
        when(
          mockedDbAdapter.execute(NEW_TEST_GRAPH.actions[0].tasks[0].statement, anything())
        ).thenResolve({
          rows: [],
          metadata: {
            bigquery: {
              jobId: "abc",
              totalBytesBilled: Long.fromNumber(0),
              totalBytesProcessed: Long.fromNumber(0)
            }
          }
        });
        when(
          mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[0].tasks[1].statement, anything())
        ).thenResolve({
          rows: [],
          metadata: {}
        });
        when(mockedDbAdapter.execute(NEW_TEST_GRAPH.actions[1].tasks[0].statement, anything()))
          .thenReject(new Error("bad statement"))
          .thenReject(new Error("bad statement"))
          .thenResolve({ rows: [], metadata: {} });

        const mockDbAdapterInstance = instance(mockedDbAdapter);
        mockDbAdapterInstance.withClientLock = async callback =>
          await callback(mockDbAdapterInstance);

        const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH);

        expect(
          dataform.RunResult.create(cleanTiming(await runner.execute().result()))
        ).to.deep.equal(EXPECTED_RUN_RESULT);
      });

      test("should pass when execution fails initially, then passes with the number of allowed retries", async () => {
        const mockedDbAdapter = mock(BigQueryDbAdapter);
        const NEW_TEST_GRAPH = {
          ...RUN_TEST_GRAPH,
          projectConfig: { ...RUN_TEST_GRAPH.projectConfig, idempotentActionRetries: 2 }
        };
        when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
        when(
          mockedDbAdapter.execute(NEW_TEST_GRAPH.actions[0].tasks[0].statement, anything())
        ).thenResolve({
          rows: [],
          metadata: {
            bigquery: {
              jobId: "abc",
              totalBytesBilled: Long.fromNumber(0),
              totalBytesProcessed: Long.fromNumber(0)
            }
          }
        });
        when(
          mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[0].tasks[1].statement, anything())
        ).thenResolve({
          rows: [],
          metadata: {}
        });
        when(mockedDbAdapter.execute(NEW_TEST_GRAPH.actions[1].tasks[0].statement, anything()))
          .thenReject(new Error("bad statement"))
          .thenReject(new Error("bad statement"))
          .thenResolve({ rows: [], metadata: {} });

        const mockDbAdapterInstance = instance(mockedDbAdapter);
        mockDbAdapterInstance.withClientLock = async callback =>
          await callback(mockDbAdapterInstance);

        const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH);

        expect(
          dataform.RunResult.create(cleanTiming(await runner.execute().result()))
        ).to.deep.equal(
          dataform.RunResult.create({
            status: dataform.RunResult.ExecutionStatus.SUCCESSFUL,
            actions: [
              EXPECTED_RUN_RESULT.actions[0],
              {
                name: NEW_TEST_GRAPH.actions[1].name,
                target: NEW_TEST_GRAPH.actions[1].target,
                tasks: [
                  {
                    status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
                    metadata: {}
                  }
                ],
                status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL
              }
            ]
          })
        );
      });

      test("should not retry when the task is an operation", async () => {
        const mockedDbAdapter = mock(BigQueryDbAdapter);
        const NEW_TEST_GRAPH_WITH_OPERATION = {
          ...RUN_TEST_GRAPH,
          projectConfig: { ...RUN_TEST_GRAPH.projectConfig, idempotentActionRetries: 3 }
        };
        NEW_TEST_GRAPH_WITH_OPERATION.actions[1].tasks[0].type = "operation";

        when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
        when(
          mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[0].tasks[0].statement, anything())
        ).thenResolve({
          rows: [],
          metadata: {
            bigquery: {
              jobId: "abc",
              totalBytesBilled: Long.fromNumber(0),
              totalBytesProcessed: Long.fromNumber(0)
            }
          }
        });
        when(
          mockedDbAdapter.execute(RUN_TEST_GRAPH.actions[0].tasks[1].statement, anything())
        ).thenResolve({
          rows: [],
          metadata: {}
        });
        when(
          mockedDbAdapter.execute(
            NEW_TEST_GRAPH_WITH_OPERATION.actions[1].tasks[0].statement,
            anything()
          )
        )
          .thenReject(new Error("bad statement"))
          .thenReject(new Error("bad statement"))
          .thenResolve({ rows: [], metadata: {} });

        const mockDbAdapterInstance = instance(mockedDbAdapter);
        mockDbAdapterInstance.withClientLock = async callback =>
          await callback(mockDbAdapterInstance);

        const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH_WITH_OPERATION);

        expect(
          dataform.RunResult.create(cleanTiming(await runner.execute().result()))
        ).to.deep.equal(EXPECTED_RUN_RESULT);
      });
    });

    test("execute_with_cancel", async () => {
      const CANCEL_TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
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
        execute: (_, { onCancel }) =>
          new Promise((__, reject) => {
            onCancel(() => {
              wasCancelled = true;
              reject(new Error("Run cancelled"));
            });
          }),
        withClientLock: callback => callback(mockDbAdapter),
        schemas: _ => Promise.resolve([]),
        createSchema: (_, __) => Promise.resolve(),
        close: () => undefined,
        table: _ => undefined
      } as IDbAdapter;

      const runner = new Runner(mockDbAdapter, CANCEL_TEST_GRAPH);
      const execution = runner.execute().result();
      // We want to await the return promise before we actually call cancel.
      // Waiting a short (10ms) time before calling cancel accomplishes this.
      await sleep(10);
      runner.cancel();
      const result = await execution;
      expect(wasCancelled).equals(true);
      // Cancelling a run doesn't actually throw at the top level.
      // The action should fail, and have an appropriate error message.
      expect(result.actions[0].tasks[0].status).equal(
        dataform.TaskResult.ExecutionStatus.CANCELLED
      );
      expect(result.actions[0].tasks[0].errorMessage).to.match(/cancelled/);
    });
  });
});

function cleanTiming(runResult: dataform.IRunResult) {
  const newRunResult = dataform.RunResult.create(runResult);
  delete newRunResult.timing;
  newRunResult.actions.forEach(actionResult => {
    delete actionResult.timing;
    actionResult.tasks.forEach(taskResult => {
      delete taskResult.timing;
    });
  });
  return newRunResult;
}
