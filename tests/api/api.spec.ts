import { Builder, compile, credentials, format, query, Runner } from "@dataform/api";
import { IDbAdapter } from "@dataform/api/dbadapters";
import { BigQueryDbAdapter } from "@dataform/api/dbadapters/bigquery";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { fail } from "assert";
import { assert, config, expect } from "chai";
import { asPlainObject, cleanSql } from "df/tests/utils";
import * as path from "path";
import * as stackTrace from "stack-trace";
import { anyString, anything, instance, mock, when } from "ts-mockito";

config.truncateThreshold = 0;

describe("@dataform/api", () => {
  describe("build", () => {
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

    it("include_deps", () => {
      const builder = new Builder(
        TEST_GRAPH,
        { actions: ["schema.a"], includeDependencies: true },
        TEST_STATE
      );
      const executionGraph = builder.build();
      const includedActionNames = executionGraph.actions.map(n => n.name);
      expect(includedActionNames).includes("schema.a");
      expect(includedActionNames).includes("schema.b");
    });

    it("exclude_deps", () => {
      const builder = new Builder(
        TEST_GRAPH,
        { actions: ["schema.a"], includeDependencies: false },
        TEST_STATE
      );
      const executionGraph = builder.build();
      const includedActionNames = executionGraph.actions.map(n => n.name);
      expect(includedActionNames).includes("schema.a");
      expect(includedActionNames).not.includes("schema.b");
    });

    it("exclude_disabled", () => {
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

      const actionNames = executedGraph.actions.map(action => action.name);

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
      expect(executionGraph.actions.filter(n => n.name === "incremental")).is.not.empty;
      expect(
        cleanSql(executionGraph.actions.filter(n => n.name === "incremental")[0].tasks[0].statement)
      ).equals(
        cleanSql(
          `insert into \`schema.incremental\` (existing_field)
           select existing_field from (
             select * from (select 1 as test) as subquery
             where true
           ) as insertions`
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

  describe("query", () => {
    it("bigquery_example", () => {
      return query
        .compile('select 1 from ${ref("example_view")}', {
          projectDir: "df/examples/common_v1",
          projectConfigOverride: { warehouse: "bigquery", gcloudProjectId: "tada-analytics" }
        })
        .then(compiledQuery => {
          expect(compiledQuery).equals(
            "select 1 from `tada-analytics.df_integration_test.example_view`"
          );
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
        mockedDbAdapter.execute(TEST_GRAPH.actions[0].tasks[0].statement, anything())
      ).thenResolve([]);
      when(
        mockedDbAdapter.execute(TEST_GRAPH.actions[1].tasks[0].statement, anything())
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
        execute: (_, { onCancel }) =>
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

  it("formatter2", async () => {
    expect(
      await format.formatFile(
        path.resolve("df/examples/never_finishes_compiling/definitions/test.js")
      )
    ).eql("while (true) {}\n");
    expect(
      await format.formatFile(
        path.resolve("df/examples/common_v2/definitions/example_assertion_with_tags.sqlx")
      )
    ).eql(`config {
  type: "assertion",
  tags: ["tag1", "tag2"]
}

js {
  const foo =
    "something!";
}

select
  CAST(
    REGEXP_EXTRACT(
      "",
      r'^/([0-9]+)\\'/.*'
    ) AS INT64
  ) AS project_id
from
  \${
    ref({
      schema: "df_integration_test",
      name: "sample_data"
    })
  }
where
  sample = 100

---

drop something

---

alter table
  \${tempTable} rename to \${finalTableName}

pre_operations {
  grant reader on \${self()}

  ---

  select
    \${foo} as bar,
    REGEXP_EXTRACT(
      "",
      r"^/([0-9]+)\\"/.*"
    ) AS project_id
}

incremental_where {
  col in (
    select
      *
    from
      \${ref("foo")}
  )
}

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
});
