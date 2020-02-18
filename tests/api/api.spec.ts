import { Builder, compile, credentials, format, prune, query, Runner } from "@dataform/api";
import { IDbAdapter } from "@dataform/api/dbadapters";
import { BigQueryDbAdapter } from "@dataform/api/dbadapters/bigquery";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { suite, test } from "@dataform/testing";
import { assert, config, expect } from "chai";
import { asPlainObject, cleanSql } from "df/tests/utils";
import * as Long from "long";
import * as path from "path";
import { anyString, anything, instance, mock, verify, when } from "ts-mockito";

config.truncateThreshold = 0;

suite("@dataform/api", () => {
  const TEST_GRAPH: dataform.ICompiledGraph = dataform.CompiledGraph.create({
    projectConfig: { warehouse: "redshift" },
    tables: [
      {
        name: "schema.a",
        target: {
          schema: "schema",
          name: "a"
        },
        query: "query",
        dependencies: ["schema.b"]
      },
      {
        name: "schema.b",
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
        target: {
          schema: "schema",
          name: "c"
        },
        query: "query"
      }
    ]
  });

  const TEST_STATE = dataform.WarehouseState.create({ tables: [] });

  suite("build", () => {
    test("exclude_disabled", () => {
      const builder = new Builder(TEST_GRAPH, { includeDependencies: true }, TEST_STATE);
      const executionGraph = builder.build();

      const actionA = executionGraph.actions.find(n => n.name === "schema.a");
      const actionB = executionGraph.actions.find(n => n.name === "schema.b");
      const actionC = executionGraph.actions.find(n => n.name === "schema.c");

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

    test("build_with_errors", () => {
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

    test("trying to fully refresh a protected dataset fails", () => {
      const testGraph = dataform.CompiledGraph.create(TEST_GRAPH);
      testGraph.tables[0].protected = true;
      const builder = new Builder(TEST_GRAPH, { fullRefresh: true }, TEST_STATE);
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

      const builder = new Builder(graph, {}, TEST_STATE);
      const executedGraph = builder.build();

      expect(executedGraph)
        .to.have.property("actions")
        .to.be.an("array").that.is.not.empty;

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
          const action = new Builder(graph, {}, TEST_STATE).build().actions[0];
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
            })
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

      expect(prunedGraph).to.exist;
      expect(prunedGraph)
        .to.have.property("tables")
        .to.be.an("array").that.is.not.empty;

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
        ...prunedGraph.operations.map(action => action.name)
      ];
      expect(actionNames).includes("schema.a");
      expect(actionNames).includes("schema.b");
    });

    test("prune actions with --actions without dependencies", () => {
      const prunedGraph = prune(TEST_GRAPH, { actions: ["schema.a"], includeDependencies: false });
      const actionNames = [
        ...prunedGraph.tables.map(action => action.name),
        ...prunedGraph.operations.map(action => action.name)
      ];
      expect(actionNames).includes("schema.a");
      expect(actionNames).not.includes("schema.b");
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
      expect(executionGraph.actions.filter(n => n.name === "incremental")).is.not.empty;
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

      const builder = new Builder(testGraph, {}, testState);
      const executionGraph = builder.build();

      expect(executionGraph.actions)
        .to.be.an("array")
        .to.have.lengthOf(6);

      executionGraph.actions.forEach((action, index) => {
        expect(action)
          .to.have.property("tasks")
          .to.be.an("array").that.is.not.empty;

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
              statement: "create or replace table `deeb.schema.name` as select 1 as test"
            }
          ]
        }
      ];
      const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
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
              statement: "create or replace table `deeb.schema.name` as select 1 as test"
            }
          ]
        }
      ];
      const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
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

  suite("init", () => {
    test("init", { timeout: 30000 }, async function() {
      // create temp directory
      const projectDir = "examples/init";

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
    const TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
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
            database: "database2",
            schema: "schema2",
            name: "target2"
          },
          tableType: "someTableType"
        }
      ]
    });

    test("execute", async () => {
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      when(mockedDbAdapter.prepareSchema(anyString(), anyString())).thenResolve(null);
      when(
        mockedDbAdapter.execute(TEST_GRAPH.actions[0].tasks[0].statement, anything())
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
        mockedDbAdapter.execute(TEST_GRAPH.actions[1].tasks[0].statement, anything())
      ).thenReject(new Error("bad statement"));

      const runner = new Runner(instance(mockedDbAdapter), TEST_GRAPH);
      await runner.execute();
      const result = await runner.resultPromise();

      delete result.timing;
      result.actions.forEach(actionResult => {
        delete actionResult.timing;
        actionResult.tasks.forEach(taskResult => {
          delete taskResult.timing;
        });
      });

      expect(dataform.RunResult.create(result)).to.deep.equal(
        dataform.RunResult.create({
          status: dataform.RunResult.ExecutionStatus.FAILED,
          actions: [
            {
              name: TEST_GRAPH.actions[0].name,
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
                }
              ],
              status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL
            },
            {
              name: TEST_GRAPH.actions[1].name,
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
        })
      );

      verify(mockedDbAdapter.prepareSchema("database", "schema1")).once();
      verify(mockedDbAdapter.prepareSchema("database2", "schema2")).once();
    });

    suite("execute with retry", () => {
      test("should fail when execution fails too many times for the retry setting", async () => {
        const mockedDbAdapter = mock(BigQueryDbAdapter);
        const NEW_TEST_GRAPH = {
          ...TEST_GRAPH,
          projectConfig: { ...TEST_GRAPH.projectConfig, idempotentActionRetries: 1 }
        };
        when(mockedDbAdapter.prepareSchema(anyString(), anyString())).thenResolve(null);
        when(
          mockedDbAdapter.execute(NEW_TEST_GRAPH.actions[0].tasks[0].statement, anything())
        ).thenResolve({ rows: [], metadata: {} });
        when(mockedDbAdapter.execute(NEW_TEST_GRAPH.actions[1].tasks[0].statement, anything()))
          .thenReject(new Error("bad statement"))
          .thenReject(new Error("bad statement"))
          .thenResolve({ rows: [], metadata: {} });

        const runner = new Runner(instance(mockedDbAdapter), NEW_TEST_GRAPH);
        const result = await runner.execute();

        delete result.timing;
        result.actions.forEach(actionResult => {
          delete actionResult.timing;
          actionResult.tasks.forEach(taskResult => {
            delete taskResult.timing;
          });
        });

        expect(dataform.RunResult.create(result)).to.deep.equal(
          dataform.RunResult.create({
            status: dataform.RunResult.ExecutionStatus.FAILED,
            actions: [
              {
                name: NEW_TEST_GRAPH.actions[0].name,
                tasks: [
                  {
                    status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
                    metadata: {}
                  }
                ],
                status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL
              },
              {
                name: TEST_GRAPH.actions[1].name,
                tasks: [
                  {
                    status: dataform.TaskResult.ExecutionStatus.FAILED,
                    errorMessage: "bigquery error: bad statement",
                    metadata: {}
                  }
                ],
                status: dataform.ActionResult.ExecutionStatus.FAILED
              }
            ]
          })
        );
      });

      test("should pass when execution fails initially, then passes with the number of allowed retries", async () => {
        const mockedDbAdapter = mock(BigQueryDbAdapter);
        const NEW_TEST_GRAPH = {
          ...TEST_GRAPH,
          projectConfig: { ...TEST_GRAPH.projectConfig, idempotentActionRetries: 2 }
        };
        when(mockedDbAdapter.prepareSchema(anyString(), anyString())).thenResolve(null);
        when(
          mockedDbAdapter.execute(NEW_TEST_GRAPH.actions[0].tasks[0].statement, anything())
        ).thenResolve({ rows: [], metadata: {} });
        when(mockedDbAdapter.execute(NEW_TEST_GRAPH.actions[1].tasks[0].statement, anything()))
          .thenReject(new Error("bad statement"))
          .thenReject(new Error("bad statement"))
          .thenResolve({ rows: [], metadata: {} });

        const runner = new Runner(instance(mockedDbAdapter), NEW_TEST_GRAPH);
        const result = await runner.execute();

        delete result.timing;
        result.actions.forEach(actionResult => {
          delete actionResult.timing;
          actionResult.tasks.forEach(taskResult => {
            delete taskResult.timing;
          });
        });
        expect(dataform.RunResult.create(result)).to.deep.equal(
          dataform.RunResult.create({
            status: dataform.RunResult.ExecutionStatus.SUCCESSFUL,
            actions: [
              {
                name: NEW_TEST_GRAPH.actions[0].name,
                tasks: [
                  {
                    status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
                    metadata: {}
                  }
                ],
                status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL
              },
              {
                name: NEW_TEST_GRAPH.actions[1].name,
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
          ...TEST_GRAPH,
          projectConfig: { ...TEST_GRAPH.projectConfig, idempotentActionRetries: 3 }
        };
        NEW_TEST_GRAPH_WITH_OPERATION.actions[1].tasks[0].type = "operation";

        when(mockedDbAdapter.prepareSchema(anyString(), anyString())).thenResolve(null);
        when(
          mockedDbAdapter.execute(
            NEW_TEST_GRAPH_WITH_OPERATION.actions[0].tasks[0].statement,
            anything()
          )
        ).thenResolve({ rows: [], metadata: {} });
        when(
          mockedDbAdapter.execute(
            NEW_TEST_GRAPH_WITH_OPERATION.actions[1].tasks[0].statement,
            anything()
          )
        )
          .thenReject(new Error("bad statement"))
          .thenReject(new Error("bad statement"))
          .thenResolve({ rows: [], metadata: {} });

        const runner = new Runner(instance(mockedDbAdapter), NEW_TEST_GRAPH_WITH_OPERATION);
        const result = await runner.execute();

        delete result.timing;
        result.actions.forEach(actionResult => {
          delete actionResult.timing;
          actionResult.tasks.forEach(taskResult => {
            delete taskResult.timing;
          });
        });

        expect(dataform.RunResult.create(result)).to.deep.equal(
          dataform.RunResult.create({
            status: dataform.RunResult.ExecutionStatus.FAILED,
            actions: [
              {
                name: NEW_TEST_GRAPH_WITH_OPERATION.actions[0].name,
                tasks: [
                  {
                    status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL,
                    metadata: {}
                  }
                ],
                status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL
              },
              {
                name: NEW_TEST_GRAPH_WITH_OPERATION.actions[1].name,
                tasks: [
                  {
                    status: dataform.TaskResult.ExecutionStatus.FAILED,
                    errorMessage: "bigquery error: bad statement",
                    metadata: {}
                  }
                ],
                status: dataform.ActionResult.ExecutionStatus.FAILED
              }
            ]
          })
        );
      });
    });

    test("execute_with_cancel", async () => {
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
        execute: (_, { onCancel }) =>
          new Promise((_, reject) => {
            onCancel(() => {
              wasCancelled = true;
              reject(new Error("Run cancelled"));
            });
          }),
        prepareSchema: (_, __) => {
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
      expect(result.actions[0].tasks[0].status).equal(
        dataform.TaskResult.ExecutionStatus.CANCELLED
      );
      expect(result.actions[0].tasks[0].errorMessage).to.match(/cancelled/);
    });
  });

  suite("formatter", () => {
    test("correctly formats simple.sqlx", async () => {
      expect(await format.formatFile(path.resolve("examples/formatter/definitions/simple.sqlx")))
        .eql(`config {
  type: "view",
  tags: ["tag1", "tag2"]
}

js {
  const foo =
    jsFunction("table");
}

select
  1
from
  \${
    ref({
      schema: "df_integration_test",
      name: "sample_data"
    })
  }
`);
    });

    test("correctly formats multiple_queries.sqlx", async () => {
      expect(
        await format.formatFile(
          path.resolve("examples/formatter/definitions/multiple_queries.sqlx")
        )
      ).eql(`js {
  var tempTable = "yay"
  const colname = "column";

  let finalTableName = 'dkaodihwada';
}

drop something

---

alter table
  \${tempTable} rename to \${finalTableName}

---

SELECT
  SUM(IF (session_start_event, 1, 0)) AS session_index
`);
    });

    test("correctly formats bigquery_regexps.sqlx", async () => {
      expect(
        await format.formatFile(
          path.resolve("examples/formatter/definitions/bigquery_regexps.sqlx")
        )
      ).eql(`config {
  type: "operation",
  tags: ["tag1", "tag2"]
}

select
  CAST(
    REGEXP_EXTRACT("", r'^/([0-9]+)\\'/.*') AS INT64
  ) AS id,
  CAST(
    REGEXP_EXTRACT("", r"^/([0-9]+)\\"/.*") AS INT64
  ) AS id2,
  IFNULL (
    regexp_extract('', r'\\a?query=([^&]+)&*'),
    regexp_extract('', r'\\a?q=([^&]+)&*')
  ) AS id3,
  regexp_extract('bar', r'bar') as ID4
from
  \${ref("dab")}
where
  sample = 100
`);
    });

    test("correctly formats comments.sqlx", async () => {
      expect(await format.formatFile(path.resolve("examples/formatter/definitions/comments.sqlx")))
        .eql(`config {
  type: "test",
}

SELECT
  MAX(
    (
      SELECT
        SUM(
          IF(
            track.event = "event_viewed_project_with_connection",
            1,
            0
          )
        )
      FROM
        UNNEST(records)
    )
  ) > 0 as created_project,
  /* multi line
  comment      */
  2 as foo

input "something" {
  select
    1 as test
    /* something */
    /* something
    else      */
    -- and another thing
}
`);
    });
    test("Backslashes within regex don't cause 'r' prefix to separate.", async () => {
      expect(await format.formatFile(path.resolve("examples/formatter/definitions/regex.sqlx")))
        .equal(`select
  regexp_extract("", r'abc\\de\\'fg select * from self()'),
  'bar'
`);
    });
  });
});
