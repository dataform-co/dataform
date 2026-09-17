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

  test("executes dynamic DML inside procedure and does not emit static DML outside procedure for onSchemaChange", () => {
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
      const builtTasks = tasks.build();
      expect(builtTasks.length).to.equal(1);

      const fullSql = builtTasks[0].statement;
      const [procedurePart, afterProcedurePart] = fullSql.split("\nEND;\n");
      expect(procedurePart).to.include("SET dataform_columns_list =");
      expect(procedurePart).to.include("SET dataform_columns_merge =");
      expect(procedurePart).to.include("EXECUTE IMMEDIATE");
      expect(afterProcedurePart).to.not.include("insert into");
      expect(afterProcedurePart).to.not.include("merge ");
      expect(afterProcedurePart).to.not.include("field1");
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
