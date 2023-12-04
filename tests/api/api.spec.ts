import { assert, config, expect } from "chai";
import Long from "long";
import { anyString, anything, instance, mock, verify, when } from "ts-mockito";

import { Builder, credentials, prune, Runner } from "df/api";
import { IDbAdapter } from "df/api/dbadapters";
import { BigQueryDbAdapter } from "df/api/dbadapters/bigquery";
import { sleep, sleepUntil } from "df/common/promises";
import { targetAsReadableString, targetsAreEqual } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { asPlainObject, cleanSql } from "df/tests/utils";

config.truncateThreshold = 0;

suite("@dataform/api", () => {
  // c +-> b +-> a
  //       ^
  //       d
  // Made with asciiflow.com
  const TEST_GRAPH: dataform.ICompiledGraph = dataform.CompiledGraph.create({
    projectConfig: { warehouse: "redshift" },
    tables: [
      {
        type: "table",
        target: {
          schema: "schema",
          name: "a"
        },
        query: "query",
        dependencyTargets: [{ schema: "schema", name: "b" }]
      },
      {
        type: "table",
        target: {
          schema: "schema",
          name: "b"
        },
        query: "query",
        dependencyTargets: [{ schema: "schema", name: "c" }],
        disabled: true
      },
      {
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
      const builder = new Builder(TEST_GRAPH, { includeDependencies: true }, TEST_STATE);
      const executionGraph = builder.build();

      const actionA = executionGraph.actions.find(
        n => targetAsReadableString(n.target) === "schema.a"
      );
      const actionB = executionGraph.actions.find(
        n => targetAsReadableString(n.target) === "schema.b"
      );
      const actionC = executionGraph.actions.find(
        n => targetAsReadableString(n.target) === "schema.c"
      );

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
          tables: [{ target: { schema: "schema", name: "a" } }]
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
          { target: { schema: "schema", name: "a" }, type: "table" },
          {
            target: { schema: "schema", name: "b" },
            type: "incremental",
            where: "test"
          },
          { target: { schema: "schema", name: "c" }, type: "view" }
        ],
        operations: [
          {
            target: { schema: "schema", name: "d" },
            queries: ["create or replace view schema.someview as select 1 as test"]
          }
        ],
        assertions: [{ target: { schema: "schema", name: "e" } }]
      });

      const builder = new Builder(graph, {}, TEST_STATE);
      const executedGraph = builder.build();

      expect(executedGraph.actions.length).greaterThan(0);

      graph.tables.forEach((t: dataform.ITable) => {
        const action = executedGraph.actions.find(item => targetsAreEqual(item.target, t.target));
        expect(action).to.include({ type: "table", target: t.target, tableType: t.type });
      });

      graph.operations.forEach((o: dataform.IOperation) => {
        const action = executedGraph.actions.find(item => targetsAreEqual(item.target, o.target));
        expect(action).to.include({ type: "operation", target: o.target });
      });

      graph.assertions.forEach((a: dataform.IAssertion) => {
        const action = executedGraph.actions.find(item => targetsAreEqual(item.target, a.target));
        expect(action).to.include({ type: "assertion" });
      });
    });

    test("table_enum_types", () => {
      const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery" },
        tables: [
          { target: { schema: "schema", name: "a" }, enumType: dataform.TableType.TABLE },
          {
            target: { schema: "schema", name: "b" },
            enumType: dataform.TableType.INCREMENTAL,
            where: "test"
          },
          { target: { schema: "schema", name: "c" }, enumType: dataform.TableType.VIEW }
        ]
      });

      const builder = new Builder(graph, {}, TEST_STATE);
      const executedGraph = builder.build();

      expect(executedGraph.actions.length).greaterThan(0);

      graph.tables.forEach((t: dataform.ITable) => {
        const action = executedGraph.actions.find(item => targetsAreEqual(item.target, t.target));
        expect(action).to.include({
          type: "table",
          target: t.target,
          tableType: dataform.TableType[t.enumType].toLowerCase(),
        });
      });
    });

    test("table_enum_and_str_types_should_match", () => {
      const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery" },
        tables: [{
          target: { schema: "schema", name: "a" },
          enumType: dataform.TableType.TABLE,
          type: "incremental",
        }]
      });

      expect(() => new Builder(graph, {}, TEST_STATE)).to.throw(
        /Table str type "incremental" and enumType "table" are not equivalent/
      );
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
          queries: ["create or replace view schema.someview as select 1 as test"]
        },
        {
          target: { schema: "schema", name: "op_b" },
          dependencyTargets: [{ schema: "schema", name: "op_a" }],
          tags: ["tag2"],
          queries: ["create or replace view schema.someview as select 1 as test"]
        },
        {
          target: { schema: "schema", name: "op_c" },
          dependencyTargets: [{ schema: "schema", name: "op_a" }],
          tags: ["tag3"],
          queries: ["create or replace view schema.someview as select 1 as test"]
        },
        {
          target: { schema: "schema", name: "op_d" },
          tags: ["tag3"],
          queries: ["create or replace view schema.someview as select 1 as test"]
        }
      ],
      tables: [
        {
          target: { schema: "schema", name: "tab_a" },
          dependencyTargets: [{ schema: "schema", name: "op_d" }],
          tags: ["tag1", "tag2"]
        }
      ]
    });

    test("prune removes inline tables", async () => {
      const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultLocation: "US" },
        tables: [
          { target: { schema: "schema", name: "a" }, type: "table" },
          {
            target: { schema: "schema", name: "b" },
            type: "inline",
            dependencyTargets: [{ schema: "schema", name: "a" }]
          },
          {
            target: { schema: "schema", name: "c" },
            type: "table",
            dependencyTargets: [{ schema: "schema", name: "a" }]
          }
        ]
      });

      const prunedGraph = prune(graph, {});

      expect(prunedGraph.tables.length).greaterThan(0);

      const actionNames = prunedGraph.tables.map(action => targetAsReadableString(action.target));

      expect(actionNames).includes("schema.a");
      expect(actionNames).not.includes("schema.b");
      expect(actionNames).includes("schema.c");
    });

    test("prune actions with --tags (with dependencies)", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        actions: ["op_b", "op_d"],
        tags: ["tag1", "tag2", "tag4"],
        includeDependencies: true
      });
      const actionNames = [
        ...prunedGraph.tables.map(action => targetAsReadableString(action.target)),
        ...prunedGraph.operations.map(action => targetAsReadableString(action.target))
      ];
      expect(actionNames).includes("schema.op_a");
      expect(actionNames).includes("schema.op_b");
      expect(actionNames).not.includes("schema.op_c");
      expect(actionNames).includes("schema.op_d");
      expect(actionNames).includes("schema.tab_a");
    });

    test("prune actions with --tags (with dependents)", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        tags: ["tag2"],
        includeDependents: true
      });
      const actionNames = [
        ...prunedGraph.tables.map(action => targetAsReadableString(action.target)),
        ...prunedGraph.operations.map(action => targetAsReadableString(action.target))
      ];
      expect(actionNames).not.includes("schema.op_a");
      expect(actionNames).includes("schema.op_b");
      expect(actionNames).not.includes("schema.op_c");
      expect(actionNames).not.includes("schema.op_d");
      expect(actionNames).includes("schema.tab_a");
    });

    test("prune actions with dependents", () => {
      const prunedGraph = prune(TEST_GRAPH, {
        actions: ["schema.c"],
        includeDependents: true
      });
      const actionNames = [
        ...prunedGraph.tables.map(action => targetAsReadableString(action.target)),
        ...prunedGraph.operations.map(action => targetAsReadableString(action.target))
      ];
      expect(actionNames).includes("schema.a");
      expect(actionNames).includes("schema.b");
      expect(actionNames).includes("schema.c");
    });

    test("prune actions with --tags but without --actions (without dependencies or dependents)", () => {
      const prunedGraph = prune(TEST_GRAPH_WITH_TAGS, {
        tags: ["tag1", "tag2", "tag4"],
        includeDependencies: false,
        includeDependents: false
      });
      const actionNames = [
        ...prunedGraph.tables.map(action => targetAsReadableString(action.target)),
        ...prunedGraph.operations.map(action => targetAsReadableString(action.target))
      ];
      expect(actionNames).includes("schema.op_a");
      expect(actionNames).includes("schema.op_b");
      expect(actionNames).not.includes("schema.op_c");
      expect(actionNames).not.includes("schema.op_d");
      expect(actionNames).includes("schema.tab_a");
    });

    test("prune actions with --actions with dependencies", () => {
      const prunedGraph = prune(TEST_GRAPH, { actions: ["schema.a"], includeDependencies: true });
      const actionNames = [
        ...prunedGraph.tables.map(action => targetAsReadableString(action.target)),
        ...prunedGraph.operations.map(action => targetAsReadableString(action.target)),
        ...prunedGraph.assertions.map(action => targetAsReadableString(action.target))
      ];
      expect(actionNames).includes("schema.a");
      expect(actionNames).includes("schema.b");
    });

    test("prune actions with --actions without dependencies", () => {
      const prunedGraph = prune(TEST_GRAPH, { actions: ["schema.a"], includeDependencies: false });
      const actionNames = [
        ...prunedGraph.tables.map(action => targetAsReadableString(action.target)),
        ...prunedGraph.operations.map(action => targetAsReadableString(action.target)),
        ...prunedGraph.assertions.map(action => targetAsReadableString(action.target))
      ];
      expect(actionNames).includes("schema.a");
      expect(actionNames).not.includes("schema.b");
      expect(actionNames).not.includes("schema.d");
    });
  });

  suite("sql_generating", () => {
    test("bigquery_incremental", () => {
      const graph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb", defaultLocation: "US" },
        tables: [
          {
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
      const executionGraph = new Builder(graph, {}, state).build();
      expect(
        cleanSql(
          executionGraph.actions.filter(
            n => targetAsReadableString(n.target) === "schema.incremental"
          )[0].tasks[0].statement
        )
      ).equals(
        cleanSql(
          `insert into \`deeb.schema.incremental\` (\`existing_field\`)
           select \`existing_field\` from (
             select * from (select 1 as test) as subquery
             where true
           ) as insertions`
        )
      );
    });

    test("bigquery_materialized", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb", defaultLocation: "US" },
        tables: [
          {
            target: {
              schema: "schema",
              name: "materialized"
            },
            type: "view",
            query: "select 1 as test",
            materialized: true
          },
          {
            target: {
              schema: "schema",
              name: "plain"
            },
            type: "view",
            query: "select 1 as test"
          }
        ]
      });
      const expectedExecutionActions: dataform.IExecutionAction[] = [
        {
          type: "table",
          tableType: "view",
          target: {
            schema: "schema",
            name: "materialized"
          },
          tasks: [
            {
              type: "statement",
              statement:
                "create or replace materialized view `deeb.schema.materialized` as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        },
        {
          type: "table",
          tableType: "view",
          target: {
            schema: "schema",
            name: "plain"
          },
          tasks: [
            {
              type: "statement",
              statement: "create or replace view `deeb.schema.plain` as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        }
      ];
      const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
      expect(asPlainObject(executionGraph.actions)).deep.equals(
        asPlainObject(expectedExecutionActions)
      );
    });

    test("snowflake_materialized", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "snowflake" },
        tables: [
          {
            target: {
              schema: "schema",
              name: "materialized"
            },
            type: "view",
            query: "select 1 as test",
            materialized: true
          },
          {
            target: {
              schema: "schema",
              name: "plain"
            },
            type: "view",
            query: "select 1 as test"
          }
        ]
      });
      const expectedExecutionActions: dataform.IExecutionAction[] = [
        {
          type: "table",
          tableType: "view",
          target: {
            schema: "schema",
            name: "materialized"
          },
          tasks: [
            {
              type: "statement",
              statement: `create or replace materialized view "schema"."materialized" as select 1 as test`
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        },
        {
          type: "table",
          tableType: "view",
          target: {
            schema: "schema",
            name: "plain"
          },
          tasks: [
            {
              type: "statement",
              statement: `create or replace view "schema"."plain" as select 1 as test`
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        }
      ];
      const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
      expect(asPlainObject(executionGraph.actions)).deep.equals(
        asPlainObject(expectedExecutionActions)
      );
    });

    test("redshift_create", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "redshift" },
        dataformCoreVersion: "1.4.1",
        tables: [
          {
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
            type: "table",
            target: {
              schema: "schema",
              name: "redshift_without_redshift"
            },
            query: "query"
          },
          {
            type: "view",
            target: {
              schema: "schema",
              name: "redshift_view"
            },
            query: "query"
          },
          {
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

      const builder = new Builder(testGraph, {}, dataform.WarehouseState.create({}));
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

      const builder = new Builder(testGraph, {}, testState);
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
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb", defaultLocation: "US" },
        tables: [
          {
            target: {
              schema: "schema",
              name: "partitionby"
            },
            type: "table",
            query: "select 1 as test",
            bigquery: {
              partitionBy: "DATE(test)",
              clusterBy: []
            }
          },
          {
            target: {
              schema: "schema",
              name: "plain"
            },
            type: "table",
            query: "select 1 as test"
          }
        ]
      });
      const expectedExecutionActions: dataform.IExecutionAction[] = [
        {
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "partitionby"
          },
          tasks: [
            {
              type: "statement",
              statement:
                "create or replace table `deeb.schema.partitionby` partition by DATE(test) as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        },
        {
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "plain"
          },
          tasks: [
            {
              type: "statement",
              statement: "create or replace table `deeb.schema.plain` as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        }
      ];
      const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
      expect(asPlainObject(executionGraph.actions)).deep.equals(
        asPlainObject(expectedExecutionActions)
      );
    });

    test("bigquery_options", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb", defaultLocation: "US" },
        tables: [
          {
            target: {
              schema: "schema",
              name: "partitionby"
            },
            type: "table",
            query: "select 1 as test",
            bigquery: {
              partitionBy: "DATE(test)",
              clusterBy: [],
              partitionExpirationDays: 1,
              requirePartitionFilter: true
            }
          },
          {
            target: {
              schema: "schema",
              name: "plain"
            },
            type: "table",
            query: "select 1 as test"
          }
        ]
      });
      const expectedExecutionActions: dataform.IExecutionAction[] = [
        {
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "partitionby"
          },
          tasks: [
            {
              type: "statement",
              statement:
                "create or replace table `deeb.schema.partitionby` partition by DATE(test) OPTIONS(partition_expiration_days=1,require_partition_filter=true)as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        },
        {
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "plain"
          },
          tasks: [
            {
              type: "statement",
              statement: "create or replace table `deeb.schema.plain` as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        }
      ];
      const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
      expect(asPlainObject(executionGraph.actions)).deep.equals(
        asPlainObject(expectedExecutionActions)
      );
    });

    test("bigquery_clusterby", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb", defaultLocation: "US" },
        tables: [
          {
            target: {
              schema: "schema",
              name: "partitionby"
            },
            type: "table",
            query: "select 1 as test",
            bigquery: {
              partitionBy: "DATE(test)",
              clusterBy: ["name", "revenue"]
            }
          },
          {
            target: {
              schema: "schema",
              name: "plain"
            },
            type: "table",
            query: "select 1 as test"
          }
        ]
      });
      const expectedExecutionActions: dataform.IExecutionAction[] = [
        {
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "partitionby"
          },
          tasks: [
            {
              type: "statement",
              statement:
                "create or replace table `deeb.schema.partitionby` partition by DATE(test) cluster by name, revenue as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        },
        {
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "plain"
          },
          tasks: [
            {
              type: "statement",
              statement: "create or replace table `deeb.schema.plain` as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        }
      ];
      const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
      expect(asPlainObject(executionGraph.actions)).deep.equals(
        asPlainObject(expectedExecutionActions)
      );
    });

    test("bigquery_additional_options", () => {
      const testGraph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery", defaultDatabase: "deeb", defaultLocation: "US" },
        tables: [
          {
            target: {
              schema: "schema",
              name: "additional_options"
            },
            type: "table",
            query: "select 1 as test",
            bigquery: {
              additionalOptions: {
                partition_expiration_days: "1",
                require_partition_filter: "true",
                friendly_name: '"friendlyName"'
              }
            }
          },
          {
            target: {
              schema: "schema",
              name: "plain"
            },
            type: "table",
            query: "select 1 as test"
          }
        ]
      });
      const expectedExecutionActions: dataform.IExecutionAction[] = [
        {
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "additional_options"
          },
          tasks: [
            {
              type: "statement",
              statement:
                'create or replace table `deeb.schema.additional_options` OPTIONS(partition_expiration_days=1,require_partition_filter=true,friendly_name="friendlyName")as select 1 as test'
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
        },
        {
          type: "table",
          tableType: "table",
          target: {
            schema: "schema",
            name: "plain"
          },
          tasks: [
            {
              type: "statement",
              statement: "create or replace table `deeb.schema.plain` as select 1 as test"
            }
          ],
          dependencyTargets: [],
          hermeticity: dataform.ActionHermeticity.HERMETIC
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
            type: "table",
            target: {
              schema: "schema",
              name: "a"
            },
            query: "select 1 as test"
          },
          {
            type: "table",
            target: {
              schema: "schema",
              name: "b"
            },
            dependencyTargets: [{ schema: "schema", name: "a" }],
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

      const statements = executionGraph.actions.flatMap(action =>
        action.tasks.map(task => task.statement)
      );
      expect(statements).includes(`create or replace table "schema"."a" as select 1 as test`);
      expect(statements).includes(`create or replace table "schema"."b" as select 1 as test`);
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

    [{}, { wrongProperty: "" }].forEach(bigquery => {
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
        defaultDatabase: "database",
        defaultLocation: "US"
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
          tableType: "someTableType",
          dependencyTargets: []
        },
        {
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
          tableType: "someTableType",
          dependencyTargets: [
            {
              schema: "schema1",
              name: "target1"
            }
          ]
        }
      ]
    });

    const EXPECTED_RUN_RESULT = dataform.RunResult.create({
      status: dataform.RunResult.ExecutionStatus.FAILED,
      actions: [
        {
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

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
      ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
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

      expect(dataform.RunResult.create(result).toJSON()).to.deep.equal(
        dataform.RunResult.create({
          status: dataform.RunResult.ExecutionStatus.RUNNING,
          actions: [
            {
              target: EXPECTED_RUN_RESULT.actions[0].target,
              status: dataform.ActionResult.ExecutionStatus.RUNNING,
              tasks: [EXPECTED_RUN_RESULT.actions[0].tasks[0]]
            }
          ]
        }).toJSON()
      );

      runner = new Runner(mockDbAdapterInstance, RUN_TEST_GRAPH, undefined, result);

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
      ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
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
          dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
        ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
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
          dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
        ).to.deep.equal(
          dataform.RunResult.create({
            status: dataform.RunResult.ExecutionStatus.SUCCESSFUL,
            actions: [
              EXPECTED_RUN_RESULT.actions[0],
              {
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
          }).toJSON()
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
          dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
        ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
      });
    });

    test("execute_with_cancel", async () => {
      const CANCEL_TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
        projectConfig: {
          warehouse: "bigquery",
          defaultSchema: "foo",
          assertionSchema: "bar",
          defaultLocation: "US"
        },
        warehouseState: {
          tables: []
        },
        actions: [
          {
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
            tableType: "table",
            dependencyTargets: []
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

    test("continues after setMetadata fails", async () => {
      const METADATA_TEST_GRAPH: dataform.IExecutionGraph = dataform.ExecutionGraph.create({
        projectConfig: {
          warehouse: "bigquery",
          defaultSchema: "foo",
          assertionSchema: "bar",
          defaultLocation: "US"
        },
        warehouseState: {
          tables: []
        },
        actions: [
          {
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
            actionDescriptor: {
              description: "desc"
            },
            tableType: "table",
            dependencyTargets: []
          }
        ]
      });
      const mockedDbAdapter = mock(BigQueryDbAdapter);
      when(mockedDbAdapter.createSchema(anyString(), anyString())).thenResolve(null);
      when(mockedDbAdapter.execute(anything(), anything())).thenResolve({
        rows: [],
        metadata: {}
      });
      when(mockedDbAdapter.setMetadata(anything())).thenReject(
        new Error("Error during setMetadata")
      );

      const mockDbAdapterInstance = instance(mockedDbAdapter);
      mockDbAdapterInstance.withClientLock = async callback =>
        await callback(mockDbAdapterInstance);

      const runner = new Runner(mockDbAdapterInstance, METADATA_TEST_GRAPH);

      expect(
        dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
      ).to.deep.equal({
        actions: [
          {
            status: "FAILED",
            target: {
              name: "target1",
              schema: "schema1"
            },
            tasks: [
              {
                errorMessage: "Error setting metadata: Error during setMetadata",
                metadata: {},
                status: "FAILED"
              }
            ]
          }
        ],
        status: "FAILED"
      });
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
