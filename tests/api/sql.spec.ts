import { config, expect } from "chai";

import { Builder } from "df/cli/api";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { asPlainObject, cleanSql, suite, test } from "df/testing";

config.truncateThreshold = 0;

suite("@dataform/api/sql", () => {
  suite("bigquery_incremental", () => {
    const projectConfig = { warehouse: "bigquery", defaultDatabase: "deeb", defaultLocation: "US" };
    const incrementalTable = {
      target: {
        schema: "schema",
        name: "incremental"
      },
      type: "incremental",
      query: "select 1 as test",
      where: "true"
    };
    const warehouseState = dataform.WarehouseState.create({
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

    test("incremental_mode", () => {
      const graph = dataform.CompiledGraph.create({
        projectConfig,
        tables: [incrementalTable]
      });

      const executionGraph = new Builder(graph, {}, warehouseState).build();

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

    test("full refresh", () => {
      const graph = dataform.CompiledGraph.create({
        projectConfig,
        tables: [incrementalTable]
      });

      const executionGraph = new Builder(graph, { fullRefresh: true }, warehouseState).build();

      expect(
        cleanSql(
          executionGraph.actions.filter(
            n => targetAsReadableString(n.target) === "schema.incremental"
          )[0].tasks[0].statement
        )
      ).equals(cleanSql("create or replace table `deeb.schema.incremental` as select 1 as test"));
    });

    test("full refresh of a protected dataset", () => {
      const protectedIncrementalTable = {
        ...incrementalTable,
        protected: true
      };
      const graph = dataform.CompiledGraph.create({
        projectConfig,
        tables: [protectedIncrementalTable]
      });

      const executionGraph = new Builder(graph, { fullRefresh: true }, warehouseState).build();

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
        plainTableDef({ type: "view" })
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
      plainTableAction({
        tableType: "view",
        tasks: [
          {
            type: "statement",
            statement: "create or replace view `deeb.schema.plain` as select 1 as test"
          }
        ]
      })
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
        plainTableDef()
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
      plainTableAction()
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
        plainTableDef()
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
      plainTableAction()
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
        plainTableDef()
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
      plainTableAction()
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
        plainTableDef()
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
      plainTableAction()
    ];
    const executionGraph = new Builder(testGraph, {}, dataform.WarehouseState.create({})).build();
    expect(asPlainObject(executionGraph.actions)).deep.equals(
      asPlainObject(expectedExecutionActions)
    );
  });
});

function plainTableDef(override?: Partial<dataform.ITable>): dataform.ITable {
  const plainTable: dataform.ITable = {
    target: {
      schema: "schema",
      name: "plain"
    },
    type: "table",
    query: "select 1 as test"
  };
  return {
    ...plainTable,
    ...override
  };
}

function plainTableAction(
  override?: Partial<dataform.IExecutionAction>
): dataform.IExecutionAction {
  const action: dataform.IExecutionAction = {
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
  };
  return {
    ...action,
    ...override
  };
}
