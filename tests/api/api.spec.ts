import { assert, config, expect } from "chai";
import * as fs from "fs-extra";
import Long from "long";
import * as path from "path";
import { anyString, anything, instance, mock, verify, when } from "ts-mockito";

import { Builder, credentials, prune, Runner } from "df/cli/api";
import { IDbAdapter } from "df/cli/api/dbadapters";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { sleep, sleepUntil } from "df/common/promises";
import { equals } from "df/common/protos";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import { asPlainObject, cleanSql } from "df/tests/utils";

config.truncateThreshold = 0;

suite("@dataform/api", () => {
  // c +-> b +-> a
  //       ^
  //       d
  // Made with asciiflow.com
  const TEST_GRAPH: dataform.ICompiledGraph = dataform.CompiledGraph.create({
    projectConfig: { warehouse: "bigquery" },
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
          projectConfig: { warehouse: "bigquery" },
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
        projectConfig: { warehouse: "bigquery" },
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
        const action = executedGraph.actions.find(item =>
          equals(dataform.Target, item.target, t.target)
        );
        expect(action).to.include({ type: "table", target: t.target, tableType: t.type });
      });

      graph.operations.forEach((o: dataform.IOperation) => {
        const action = executedGraph.actions.find(item =>
          equals(dataform.Target, item.target, o.target)
        );
        expect(action).to.include({ type: "operation", target: o.target });
      });

      graph.assertions.forEach((a: dataform.IAssertion) => {
        const action = executedGraph.actions.find(item =>
          equals(dataform.Target, item.target, a.target)
        );
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
        const action = executedGraph.actions.find(item =>
          equals(dataform.Target, item.target, t.target)
        );
        expect(action).to.include({
          type: "table",
          target: t.target,
          tableType: dataform.TableType[t.enumType].toLowerCase()
        });
      });
    });

    test("table_enum_and_str_types_should_match", () => {
      const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
        projectConfig: { warehouse: "bigquery" },
        tables: [
          {
            target: { schema: "schema", name: "a" },
            enumType: dataform.TableType.TABLE,
            type: "incremental"
          }
        ]
      });

      expect(() => new Builder(graph, {}, TEST_STATE)).to.throw(
        /Table str type "incremental" and enumType "table" are not equivalent/
      );
    });

    suite("pre and post ops", () => {
      for (const warehouse of ["bigquery"]) {
        const graph: dataform.ICompiledGraph = dataform.CompiledGraph.create({
          projectConfig: { warehouse: "bigquery" },
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
          expect(action.tasks).eql([
            dataform.ExecutionTask.create({
              type: "statement",
              statement: "preOp\n;\ncreate or replace table `schema.a` as foo\n;\npostOp"
            })
          ]);
        });

        test(`${warehouse} when running incrementally`, () => {
          const action = new Builder(
            graph,
            {},
            dataform.WarehouseState.create({
              tables: [{ target: graph.tables[0].target, fields: [] }]
            })
          ).build().actions[0];
          expect(action.tasks).eql([
            dataform.ExecutionTask.create({
              type: "statement",
              statement:
                "incremental preOp\n;\ndrop view if exists `schema.a`\n;\ninsert into `schema.a`\t\n()\t\nselect \t\nfrom (incremental foo) as insertions\n;\nincremental postOp"
            })
          ]);
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
  });

  suite("credentials_config", ({ afterEach }) => {
    const tmpDirFixture = new TmpDirFixture(afterEach);

    test("bigquery blank credentials file", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const credentialsPath = path.join(projectDir, "credentials.json");
      fs.writeFileSync(credentialsPath, "");
      expect(() => credentials.read(credentialsPath)).to.throw(
        /Error reading credentials file: Unexpected end of JSON input/
      );
    });

    test("bigquery empty credentials file", () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const credentialsPath = path.join(projectDir, "credentials.json");
      fs.writeFileSync(credentialsPath, "{}");
      expect(() => credentials.read(credentialsPath)).to.throw(
        /Error reading credentials file: the projectId field is required/
      );
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
        const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
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

        const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
          bigquery: { actionRetryLimit: 1 }
        });

        expect(
          dataform.RunResult.create(cleanTiming(await runner.execute().result())).toJSON()
        ).to.deep.equal(EXPECTED_RUN_RESULT.toJSON());
      });

      test("should pass when execution fails initially, then passes with the number of allowed retries", async () => {
        const mockedDbAdapter = mock(BigQueryDbAdapter);
        const NEW_TEST_GRAPH = RUN_TEST_GRAPH;
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

        const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH, {
          bigquery: { actionRetryLimit: 2 }
        });

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
        const NEW_TEST_GRAPH_WITH_OPERATION = RUN_TEST_GRAPH;
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

        const runner = new Runner(mockDbAdapterInstance, NEW_TEST_GRAPH_WITH_OPERATION, {
          bigquery: { actionRetryLimit: 3 }
        });

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
