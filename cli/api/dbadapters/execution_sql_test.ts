import { expect } from "chai";

import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("ExecutionSql with 'onSchemaChange'", () => {
  const executionSql = new ExecutionSql(
    {
      defaultDatabase: "project-id",
      defaultSchema: "dataset-id"
    },
    "2.0.0"
  );

  const baseTable: dataform.ITable = {
    type: "incremental",
    enumType: dataform.TableType.INCREMENTAL,
    target: {
      database: "project-id",
      schema: "dataset-id",
      name: "incremental_on_schema_change"
    },
    query: "select 1 as id, 'a' as field1",
    incrementalQuery: "select 1 as id, 'a' as field1, 'new' as field2"
  };

  const tableMetadata: dataform.ITableMetadata = {
    type: dataform.TableMetadata.Type.TABLE,
    fields: [
      {
        name: "id",
        primitive: dataform.Field.Primitive.INTEGER
      },
      {
        name: "field1",
        primitive: dataform.Field.Primitive.STRING
      }
    ]
  };

  test("generates procedure for FAIL strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.FAIL
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const procedureSql = tasks.build()[0].statement;
    expect(procedureSql).to.match(
      /create or replace procedure `project-id.dataset-id.df_osc_.*`\(\)\s+options\(strict_mode=false\)/i
    );
    expect(procedureSql).to.include(
      `"Schema mismatch defined by on_schema_change = 'FAIL'. Added columns: %t, removed columns: %t"`
    );
    expect(procedureSql).to.match(/call `project-id.dataset-id.df_osc_.*`\(\)/i);
    expect(procedureSql).to.include("EXCEPTION WHEN ERROR THEN");
    expect(procedureSql).to.match(/drop procedure if exists `project-id.dataset-id.df_osc_.*`/i);
  });

  test("generates procedure for EXTEND strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.EXTEND
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const procedureSql = tasks.build()[0].statement;

    expect(procedureSql).to.match(
      /create or replace procedure `project-id.dataset-id.df_osc_.*`\(\)\s+options\(strict_mode=false\)/i
    );
    expect(procedureSql).to.include(
      `"Column removals are not allowed when on_schema_change = 'EXTEND'. Removed columns: %t"`
    );
    expect(procedureSql).to.include("ADD COLUMN IF NOT EXISTS");
  });

  test("generates procedure for SYNCHRONIZE strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.SYNCHRONIZE,
      uniqueKey: ["id"]
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const procedureSql = tasks.build()[0].statement;

    expect(procedureSql).to.match(
      /create or replace procedure `project-id.dataset-id.df_osc_.*`\(\)\s+options\(strict_mode=false\)/i
    );
    expect(procedureSql).to.include("ADD COLUMN IF NOT EXISTS");
    expect(procedureSql).to.include("DROP COLUMN IF EXISTS");
  });

  test("SYNCHRONIZE strategy prevents dropping unique keys", () => {
    const tableWithExtraField = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.SYNCHRONIZE,
      uniqueKey: ["field_to_be_removed"]
    };
    const tasks = executionSql.publishTasks(
      tableWithExtraField,
      { fullRefresh: false },
      {
        ...tableMetadata,
        fields: [
          { name: "field_to_be_removed", primitive: dataform.Field.Primitive.STRING }
        ]
      }
    );
    const procedureSql = tasks.build()[0].statement;
    expect(procedureSql).to.include(
      `"Cannot drop column %s as it is part of the unique key for table`
    );
  });

  test("generates simple merge for IGNORE strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.IGNORE,
      uniqueKey: ["id"]
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const mergeSql = tasks.build()[0].statement;
    expect(mergeSql).to.include("merge `project-id.dataset-id.incremental_on_schema_change` T");
    expect(mergeSql).to.not.include("create or replace procedure");
  });
});
