import { expect } from "chai";
import * as fs from "fs-extra";

import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("ExecutionSql with 'onSchemaChange'", () => {
  const executionSql = new ExecutionSql(
    {
      defaultDatabase: "project-id",
      defaultSchema: "dataset-id",
    },
    "2.0.0",
    () => "test_uuid",
  );

  const baseTable: dataform.ITable = {
    type: "incremental",
    enumType: dataform.TableType.INCREMENTAL,
    target: {
      database: "project-id",
      schema: "dataset-id",
      name: "incremental_on_schema_change",
    },
    query: "select 1 as id, 'a' as field1",
    incrementalQuery: "select 1 as id, 'a' as field1, 'new' as field2",
  };

  const tableMetadata: dataform.ITableMetadata = {
    type: dataform.TableMetadata.Type.TABLE,
    fields: [
      {
        name: "id",
        primitive: dataform.Field.Primitive.INTEGER,
      },
      {
        name: "field1",
        primitive: dataform.Field.Primitive.STRING,
      },
    ],
  };

  test("generates procedure for FAIL strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.FAIL,
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const procedureSql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/on_schema_change_fail.sql", "utf8");
    expect(procedureSql).to.equal(expectedSql.trim());
  });

  test("generates procedure for EXTEND strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.EXTEND,
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const procedureSql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/on_schema_change_extend.sql", "utf8");
    expect(procedureSql).to.equal(expectedSql.trim());
  });

  test("generates procedure for SYNCHRONIZE strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.SYNCHRONIZE,
      uniqueKey: ["id"],
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const procedureSql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/on_schema_change_synchronize.sql", "utf8");
    expect(procedureSql).to.equal(expectedSql.trim());
  });

  test("generates simple merge for IGNORE strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.IGNORE,
      uniqueKey: ["id"],
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const procedureSql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/on_schema_change_ignore.sql", "utf8");
    expect(procedureSql).to.equal(expectedSql.trim());
  });

  test("generates INSERT_OVERWRITE script for IGNORE strategy", () => {
    const table = {
      ...baseTable,
      incrementalStrategy: dataform.IncrementalStrategy.INSERT_OVERWRITE,
      bigquery: {
        partitionBy: "DATE(ts)",
        incrementalPredicates: [
          "DATAFORM_DEST.ts >= '2024-01-01'",
          "DATAFORM_SOURCE.ts >= '2024-01-01'",
        ],
      },
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const sql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/insert_overwrite_ignore.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("generates INSERT_OVERWRITE script for EXTEND strategy", () => {
    const table = {
      ...baseTable,
      incrementalStrategy: dataform.IncrementalStrategy.INSERT_OVERWRITE,
      onSchemaChange: dataform.OnSchemaChange.EXTEND,
      bigquery: {
        partitionBy: "DATE(ts)",
      },
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const sql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/insert_overwrite_extend.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("places all DECLARE statements before any executable statement in generated procedure body", () => {
    for (const strategy of [
      dataform.OnSchemaChange.FAIL,
      dataform.OnSchemaChange.EXTEND,
      dataform.OnSchemaChange.SYNCHRONIZE,
    ]) {
      const table = {
        ...baseTable,
        onSchemaChange: strategy,
        uniqueKey: ["id"],
      };
      const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
      const createProcedureSql = tasks.build()[0].statement;
      expect(createProcedureSql).to.include("CREATE OR REPLACE PROCEDURE");

      const procedureBody = createProcedureSql.split("BEGIN\n")[1].split("\nEND;")[0];
      const statements = procedureBody
        .split(";")
        .map((s) =>
          s
            .split("\n")
            .filter((line) => !line.trim().startsWith("--"))
            .join("\n")
            .trim(),
        )
        .filter((s) => s.length > 0);

      let seenNonDeclare = false;
      for (const stmt of statements) {
        if (stmt.toUpperCase().startsWith("DECLARE ")) {
          expect(
            seenNonDeclare,
            `DECLARE statement appeared after non-DECLARE statement in strategy ${dataform.OnSchemaChange[strategy]}: "${stmt}"`,
          ).to.equal(false);
        } else {
          seenNonDeclare = true;
        }
      }
    }
  });
});

suite("ExecutionSql for property graphs", () => {
  const executionSql = new ExecutionSql(
    {
      defaultDatabase: "project-id",
      defaultSchema: "dataset-id",
    },
    "2.0.0",
    () => "test_uuid",
  );

  test("emits CREATE OR REPLACE PROPERTY GRAPH for FinGraph", () => {
    const graphBody = `NODE TABLES (
  \`project-id.dataset-id.account\` AS Account KEY (id) LABEL Account PROPERTIES ARE ALL COLUMNS,
  \`project-id.dataset-id.person\` AS Person KEY (id) LABEL Person PROPERTIES ARE ALL COLUMNS
)
EDGE TABLES (
  \`project-id.dataset-id.person_own_account\` AS PersonOwnAccount SOURCE KEY (id) REFERENCES Person (id) DESTINATION KEY (account_id) REFERENCES Account (id) LABEL Owns PROPERTIES ARE ALL COLUMNS,
  \`project-id.dataset-id.account_transfer_account\` AS AccountTransferAccount SOURCE KEY (id) REFERENCES Account (id) DESTINATION KEY (to_id) REFERENCES Account (id) LABEL Transfers PROPERTIES (amount, create_time)
)`;
    const propertyGraph: dataform.IPropertyGraph = {
      target: { database: "project-id", schema: "dataset-id", name: "FinGraph" },
      graphBody,
    };
    const tasks = executionSql.createPropertyGraphTasks(propertyGraph);
    const sql = tasks.map((t) => t.statement).join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/property_graph_fingraph.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("emits CREATE OR REPLACE PROPERTY GRAPH for HRGraph", () => {
    const graphBody = `NODE TABLES (
  \`project-id.dataset-id.employee\` AS Employee KEY (id) LABEL Employee PROPERTIES ARE ALL COLUMNS EXCEPT (ssn, salary),
  \`project-id.dataset-id.manager\` AS Manager KEY (id) LABEL Manager PROPERTIES ARE ALL COLUMNS EXCEPT (bonus)
)
EDGE TABLES (
  \`project-id.dataset-id.reports\` AS Reports SOURCE KEY (employee_id) REFERENCES Employee (id) DESTINATION KEY (manager_id) REFERENCES Manager (id) LABEL Reports PROPERTIES ARE ALL COLUMNS
)`;
    const propertyGraph: dataform.IPropertyGraph = {
      target: { database: "project-id", schema: "dataset-id", name: "HRGraph" },
      graphBody,
    };
    const tasks = executionSql.createPropertyGraphTasks(propertyGraph);
    const sql = tasks.map((t) => t.statement).join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/property_graph_hrgraph.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });
});

suite("ExecutionSql for operations", () => {
  const executionSql = new ExecutionSql(
    { defaultDatabase: "project-id", defaultSchema: "dataset-id" },
    "2.0.0",
  );
  const target = { database: "project-id", schema: "dataset-id", name: "op" };

  test("runs all queries as a single script", () => {
    expect(
      executionSql.createOperationTasks({
        target,
        queries: ["DECLARE min_id INT64 DEFAULT 0", "SELECT * FROM t WHERE id >= min_id -- c"],
      }),
    ).deep.equals([
      dataform.ExecutionTask.create({
        type: "statement",
        statement: "DECLARE min_id INT64 DEFAULT 0;\nSELECT * FROM t WHERE id >= min_id -- c",
      }),
    ]);
  });

  test("terminates queries ending with a comment, and keeps existing semicolons", () => {
    expect(
      executionSql.createOperationTasks({
        target,
        queries: ["\n  CREATE TABLE a AS SELECT 1 -- comment\n", "DROP TABLE b;", "SELECT 2"],
      })[0].statement,
    ).equals("CREATE TABLE a AS SELECT 1; -- comment\nDROP TABLE b;\nSELECT 2");
  });

  test("uses a single query as is", () => {
    const query = "\nSELECT '--' AS dashes -- comment\n";
    expect(executionSql.createOperationTasks({ target, queries: [query] })).deep.equals([
      dataform.ExecutionTask.create({ type: "statement", statement: query }),
    ]);
  });

  test("skips empty queries", () => {
    expect(
      executionSql.createOperationTasks({ target, queries: ["  \n", "SELECT 1", ""] }),
    ).deep.equals([dataform.ExecutionTask.create({ type: "statement", statement: "SELECT 1" })]);
    expect(executionSql.createOperationTasks({ target, queries: [] })).deep.equals([]);
  });

  test("returns no tasks for disabled operations", () => {
    expect(
      executionSql.createOperationTasks({ target, queries: ["SELECT 1"], disabled: true }),
    ).deep.equals([]);
  });
});

suite("ExecutionSql for 4-part Lakehouse targets", () => {
  const executionSql = new ExecutionSql(
    {
      defaultDatabase: "project-id",
      defaultSchema: "default_dataset",
    },
    "3.1.5",
    () => "test_uuid",
  );

  const lakehouseTarget: dataform.ITarget = {
    database: "project-id",
    schema: "ecommerce_catalog.sales_dw",
    name: "customer_orders",
  };

  const lakehouseMetadata: dataform.ITableMetadata = {
    type: dataform.TableMetadata.Type.TABLE,
    target: lakehouseTarget,
    fields: [
      { name: "order_id", primitive: dataform.Field.Primitive.INTEGER },
      { name: "customer_id", primitive: dataform.Field.Primitive.INTEGER },
    ],
  };

  test("emits 4-part CREATE OR REPLACE TABLE and skips incompatible-type DROP VIEW IF EXISTS", () => {
    const table: dataform.ITable = {
      type: "table",
      enumType: dataform.TableType.TABLE,
      target: lakehouseTarget,
      query: "SELECT 1 AS order_id, 2 AS customer_id",
      bigquery: {
        partitionBy: "BUCKET(order_id, 16)",
      },
    };
    const incompatibleMetadata: dataform.ITableMetadata = {
      type: dataform.TableMetadata.Type.UNKNOWN,
      target: lakehouseTarget,
      fields: [],
    };

    const tasks = executionSql
      .publishTasks(table, { fullRefresh: false }, incompatibleMetadata)
      .build();

    expect(tasks.length).to.equal(1);
    expect(tasks[0].statement).to.not.include("drop view");
    expect(tasks[0].statement).to.equal(
      "create or replace table `project-id.ecommerce_catalog.sales_dw.customer_orders` " +
        "partition by BUCKET(order_id, 16) as SELECT 1 AS order_id, 2 AS customer_id",
    );
  });

  test("emits CREATE OR REPLACE TABLE on first run and fullRefresh of 4-part incremental table", () => {
    const incTable: dataform.ITable = {
      type: "incremental",
      enumType: dataform.TableType.INCREMENTAL,
      target: lakehouseTarget,
      query: "SELECT 1 AS order_id, 2 AS customer_id",
      incrementalQuery: "SELECT 3 AS order_id, 4 AS customer_id",
    };

    const firstRunSql = executionSql
      .publishTasks(incTable, { fullRefresh: false })
      .build()[0].statement;
    const fullRefreshSql = executionSql
      .publishTasks(incTable, { fullRefresh: true }, lakehouseMetadata)
      .build()[0].statement;

    const expectedDdl =
      "create or replace table `project-id.ecommerce_catalog.sales_dw.customer_orders` " +
      "as SELECT 1 AS order_id, 2 AS customer_id";
    expect(firstRunSql).to.equal(expectedDdl);
    expect(fullRefreshSql).to.equal(expectedDdl);
  });

  test("emits valid 4-part DML for incremental INSERT, MERGE, and INSERT_OVERWRITE", () => {
    const baseIncTable: dataform.ITable = {
      type: "incremental",
      enumType: dataform.TableType.INCREMENTAL,
      target: lakehouseTarget,
      query: "SELECT 1 AS order_id, 2 AS customer_id",
      incrementalQuery: "SELECT 3 AS order_id, 4 AS customer_id",
    };

    const insertSql = executionSql
      .publishTasks(baseIncTable, { fullRefresh: false }, lakehouseMetadata)
      .build()[0].statement;
    expect(insertSql).to.include(
      "insert into `project-id.ecommerce_catalog.sales_dw.customer_orders`",
    );
    expect(insertSql).to.include("(`order_id`,`customer_id`)");

    const mergeSql = executionSql
      .publishTasks(
        { ...baseIncTable, uniqueKey: ["order_id"] },
        { fullRefresh: false },
        lakehouseMetadata,
      )
      .build()[0].statement;
    expect(mergeSql).to.include(
      "merge `project-id.ecommerce_catalog.sales_dw.customer_orders` DATAFORM_DEST",
    );

    const insertOverwriteSql = executionSql
      .publishTasks(
        {
          ...baseIncTable,
          incrementalStrategy: dataform.IncrementalStrategy.INSERT_OVERWRITE,
          bigquery: { partitionBy: "DATE(ts)" },
        },
        { fullRefresh: false },
        lakehouseMetadata,
      )
      .build()[0].statement;
    expect(insertOverwriteSql).to.include(
      "CREATE OR REPLACE TEMP TABLE `staging_table_temp_test_uuid`",
    );
    expect(insertOverwriteSql).to.include(
      "MERGE `project-id.ecommerce_catalog.sales_dw.customer_orders` DATAFORM_DEST",
    );
  });

  test("handles onSchemaChange = FAIL on 4-part target without procedure in catalog.namespace or 5-part INFORMATION_SCHEMA", () => {
    const table: dataform.ITable = {
      type: "incremental",
      enumType: dataform.TableType.INCREMENTAL,
      target: lakehouseTarget,
      query: "SELECT 1 AS order_id, 2 AS customer_id",
      incrementalQuery: "SELECT 1 AS order_id, 2 AS customer_id, 'new' AS status",
      onSchemaChange: dataform.OnSchemaChange.FAIL,
    };

    const sql = executionSql
      .publishTasks(table, { fullRefresh: false }, lakehouseMetadata)
      .build()[0].statement;

    expect(sql).to.not.include("CREATE OR REPLACE PROCEDURE");
    expect(sql).to.not.include("ecommerce_catalog.sales_dw.INFORMATION_SCHEMA");
    expect(sql).to.not.include("ecommerce_catalog.sales_dw.df_osc_");
    expect(sql).to.include(
      "CREATE OR REPLACE TABLE `project-id.default_dataset.customer_orders_df_temp_test_uuid_empty`",
    );
    expect(sql).to.include("SET dataform_columns = ARRAY<STRING>['order_id', 'customer_id'];");
    expect(sql).to.include("FROM `project-id.default_dataset.INFORMATION_SCHEMA.COLUMNS`");
    expect(sql).to.include("Schema mismatch defined by on_schema_change = 'FAIL'.");
    expect(sql).to.include(
      "EXCEPTION WHEN ERROR THEN\n  DROP TABLE IF EXISTS `project-id.default_dataset.customer_orders_df_temp_test_uuid_empty`;\n  RAISE;\nEND;",
    );
  });

  test("handles onSchemaChange = EXTEND on 4-part target and applies schemaSuffix to scratch dataset", () => {
    const suffixedExecutionSql = new ExecutionSql(
      {
        defaultDatabase: "project-id",
        defaultSchema: "default_dataset",
        schemaSuffix: "staging",
      },
      "3.1.5",
      () => "test_uuid",
    );
    const table: dataform.ITable = {
      type: "incremental",
      enumType: dataform.TableType.INCREMENTAL,
      target: lakehouseTarget,
      query: "SELECT 1 AS order_id, 2 AS customer_id",
      incrementalQuery: "SELECT 1 AS order_id, 2 AS customer_id, 'new' AS status",
      onSchemaChange: dataform.OnSchemaChange.EXTEND,
    };

    const sql = suffixedExecutionSql
      .publishTasks(table, { fullRefresh: false }, lakehouseMetadata)
      .build()[0].statement;

    expect(sql).to.not.include("CREATE OR REPLACE PROCEDURE");
    expect(sql).to.not.include("ecommerce_catalog.sales_dw.INFORMATION_SCHEMA");
    expect(sql).to.include(
      "CREATE OR REPLACE TABLE `project-id.default_dataset_staging.customer_orders_df_temp_test_uuid_empty`",
    );
    expect(sql).to.include("FROM `project-id.default_dataset_staging.INFORMATION_SCHEMA.COLUMNS`");
    expect(sql).to.include(
      '"ALTER TABLE `project-id.ecommerce_catalog.sales_dw.customer_orders` "',
    );
    expect(sql).to.include("ADD COLUMN IF NOT EXISTS");
  });

  test("handles onSchemaChange = SYNCHRONIZE on 4-part target with DECLARE statements first in BEGIN block", () => {
    const table: dataform.ITable = {
      type: "incremental",
      enumType: dataform.TableType.INCREMENTAL,
      target: lakehouseTarget,
      uniqueKey: ["order_id"],
      query: "SELECT 1 AS order_id, 2 AS customer_id",
      incrementalQuery: "SELECT 1 AS order_id, 'new' AS status",
      onSchemaChange: dataform.OnSchemaChange.SYNCHRONIZE,
    };

    const sql = executionSql
      .publishTasks(table, { fullRefresh: false }, lakehouseMetadata)
      .build()[0].statement;

    expect(sql).to.not.include("CREATE OR REPLACE PROCEDURE");
    expect(sql).to.not.include("ecommerce_catalog.sales_dw.INFORMATION_SCHEMA");
    expect(sql).to.include("DECLARE invalid_removed_columns ARRAY<STRING>;");
    expect(sql).to.include(
      "Cannot drop columns %T as they are part of the unique key for table `project-id.ecommerce_catalog.sales_dw.customer_orders`",
    );
    expect(sql).to.include("DROP COLUMN IF EXISTS");
    expect(sql).to.include("ADD COLUMN IF NOT EXISTS");

    const scriptBody = sql.split("BEGIN\n")[1].split("\nEXCEPTION WHEN ERROR THEN")[0];
    const statements = scriptBody
      .split(";")
      .map((s) =>
        s
          .split("\n")
          .filter((line) => !line.trim().startsWith("--"))
          .join("\n")
          .trim(),
      )
      .filter((s) => s.length > 0);

    let seenNonDeclare = false;
    for (const stmt of statements) {
      if (stmt.toUpperCase().startsWith("DECLARE ")) {
        expect(seenNonDeclare).to.equal(false);
      } else {
        seenNonDeclare = true;
      }
    }
  });

  test("creates operation tasks for 4-part Lakehouse operation target", () => {
    const tasks = executionSql.createOperationTasks({
      target: lakehouseTarget,
      hasOutput: true,
      queries: [
        "CREATE OR REPLACE TABLE `project-id.ecommerce_catalog.sales_dw.customer_orders` (order_id INT64)",
      ],
    });
    expect(tasks).deep.equals([
      dataform.ExecutionTask.create({
        type: "statement",
        statement:
          "CREATE OR REPLACE TABLE `project-id.ecommerce_catalog.sales_dw.customer_orders` (order_id INT64)",
      }),
    ]);
  });
});
