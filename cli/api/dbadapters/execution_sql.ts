import * as crypto from "crypto";
import * as semver from "semver";

import { concatenateQueries, Task, Tasks } from "df/cli/api/dbadapters/tasks";
import { ErrorWithCause } from "df/common/errors/errors";
import { CompilationSql } from "df/core/compilation_sql";
import { tableTypeEnumToString } from "df/core/utils";
import { dataform } from "df/protos/ts";

export type QueryOrAction = string | dataform.Table | dataform.Operation | dataform.Assertion;

export interface IValidationQuery {
  query?: string;
  incremental?: boolean;
}

export class ExecutionSql {
  private readonly CompilationSql: CompilationSql;

  constructor(
    private readonly project: dataform.IProjectConfig,
    private readonly dataformCoreVersion: string
  ) {
    this.CompilationSql = new CompilationSql(project, dataformCoreVersion);
  }

  public baseTableType(enumType: dataform.TableType) {
    switch (enumType) {
      case dataform.TableType.TABLE:
      case dataform.TableType.INCREMENTAL:
        return dataform.TableMetadata.Type.TABLE;
      case dataform.TableType.VIEW:
        return dataform.TableMetadata.Type.VIEW;
      default:
        throw new Error(`Unexpected table type: ${tableTypeEnumToString(enumType)}`);
    }
  }

  public tableTypeAsSql(type: dataform.TableMetadata.Type) {
    switch (type) {
      case dataform.TableMetadata.Type.TABLE:
        return "table";
      case dataform.TableMetadata.Type.VIEW:
        return "view";
      default:
        throw new Error(`Unexpected table type: ${type}`);
    }
  }

  public insertInto(target: dataform.ITarget, columns: string[], query: string) {
    return `	
insert into ${this.resolveTarget(target)}	
(${columns.join(",")})	
select ${columns.join(",")}	
from (${query}) as insertions`;
  }

  public oppositeTableType(type: dataform.TableMetadata.Type) {
    switch (type) {
      case dataform.TableMetadata.Type.TABLE:
        return dataform.TableMetadata.Type.VIEW;
      case dataform.TableMetadata.Type.VIEW:
        return dataform.TableMetadata.Type.TABLE;
      default:
        throw new Error(`Unexpected table type: ${type}`);
    }
  }

  public where(query: string, where: string) {
    return where
      ? `
  select * from (${query}) as subquery
    where ${where}`
      : query;
  }

  public shouldWriteIncrementally(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata
  ) {
    return (
      (!runConfig.fullRefresh || table.protected) &&
      tableMetadata &&
      tableMetadata.type !== dataform.TableMetadata.Type.VIEW
    );
  }

  public preOps(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata
  ): Task[] {
    let preOps = table.preOps;
    if (
      semver.gt(this.dataformCoreVersion, "1.4.8") &&
      table.enumType === dataform.TableType.INCREMENTAL &&
      this.shouldWriteIncrementally(table, runConfig, tableMetadata)
    ) {
      preOps = table.incrementalPreOps;
    }
    return (preOps || []).map(pre => Task.statement(pre));
  }

  public postOps(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata
  ): Task[] {
    let postOps = table.postOps;
    if (
      semver.gt(this.dataformCoreVersion, "1.4.8") &&
      table.enumType === dataform.TableType.INCREMENTAL &&
      this.shouldWriteIncrementally(table, runConfig, tableMetadata)
    ) {
      postOps = table.incrementalPostOps;
    }
    return (postOps || []).map(post => Task.statement(post));
  }

  public resolveTarget(target: dataform.ITarget) {
    return this.CompilationSql.resolveTarget(target);
  }

  public publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata
  ): Tasks {
    const tasks = new Tasks();

    this.preOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    const baseTableType = this.baseTableType(table.enumType);
    if (tableMetadata && tableMetadata.type !== baseTableType) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(baseTableType)))
      );
    }

    if (table.enumType === dataform.TableType.INCREMENTAL) {
      if (!this.shouldWriteIncrementally(table, runConfig, tableMetadata)) {
        tasks.add(Task.statement(this.createOrReplace(table)));
      } else {
        const onSchemaChange = table.onSchemaChange || dataform.OnSchemaChange.IGNORE;
        switch (onSchemaChange) {
          case dataform.OnSchemaChange.FAIL:
          case dataform.OnSchemaChange.EXTEND:
          case dataform.OnSchemaChange.SYNCHRONIZE:
            const uniqueId = crypto.randomUUID().replace(/-/g, "_");

            const shortEmptyTableName = `${table.target.name}_df_temp_${uniqueId}_empty`;
            const emptyTempTableName = this.resolveTarget({
              ...table.target,
              name: shortEmptyTableName
            });

            const shortDataTableName = shortEmptyTableName.replace("_empty", "_data");
            const dataTempTableName = this.resolveTarget({
              ...table.target,
              name: shortDataTableName
            });

            const procedureName = this.createProcedureName(table.target, uniqueId);
            const procedureBody = this.incrementalSchemaChangeBody(
              table,
              this.resolveTarget(table.target),
              emptyTempTableName,
              dataTempTableName,
              shortEmptyTableName
            );

            const createProcedureSql = `CREATE OR REPLACE PROCEDURE ${procedureName}()
OPTIONS(strict_mode=false)
BEGIN
${procedureBody}
END;`;
            const callProcedureSql = this.safeCallProcedure(
              procedureName,
              emptyTempTableName,
              dataTempTableName
            );
            tasks.add(Task.statement(createProcedureSql + "\n" + callProcedureSql));
            break;
          case dataform.OnSchemaChange.IGNORE:
          default:
            tasks.add(
              Task.statement(
                table.uniqueKey && table.uniqueKey.length > 0
                  ? this.mergeInto(
                      table.target,
                      tableMetadata?.fields.map(f => f.name),
                      this.where(table.incrementalQuery || table.query, table.where),
                      table.uniqueKey,
                      table.bigquery && table.bigquery.updatePartitionFilter
                    )
                  : this.insertInto(
                      table.target,
                      tableMetadata?.fields.map(f => f.name).map(column => `\`${column}\``),
                      this.where(table.incrementalQuery || table.query, table.where)
                    )
              )
            );
            break;
        }
      }
    } else {
      tasks.add(Task.statement(this.createOrReplace(table)));
    }

    this.postOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    return tasks.concatenate();
  }

  public assertTasks(
    assertion: dataform.IAssertion,
    projectConfig: dataform.IProjectConfig,
  ): Tasks {
    const tasks = new Tasks();
    const target = assertion.target;
    // Create the view to check syntax of assertion
    tasks.add(Task.statement(this.createOrReplaceView(target, assertion.query)));

    // Add assertion check
    tasks.add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
    return tasks;
  }

  public dropIfExists(target: dataform.ITarget, type: dataform.TableMetadata.Type) {
    return `drop ${this.tableTypeAsSql(type)} if exists ${this.resolveTarget(target)}`;
  }

    private createProcedureName(target: dataform.ITarget, uniqueId: string): string {
    // Procedure names cannot contain hyphens.
    const sanitizedUniqueId = uniqueId.replace(/-/g, "_");
    return this.resolveTarget({
      ...target,
      name: `df_osc_${sanitizedUniqueId}`
    });
  }

  private safeCallProcedure(
    procedureName: string,
    emptyTempTableName: string,
    dataTempTableName: string
  ): string {
    return `
BEGIN
  CALL ${procedureName}();
EXCEPTION WHEN ERROR THEN
  DROP TABLE IF EXISTS ${emptyTempTableName};
  DROP TABLE IF EXISTS ${dataTempTableName};
  DROP PROCEDURE IF EXISTS ${procedureName};
  RAISE;
END;
DROP PROCEDURE IF EXISTS ${procedureName};`;
  }

  private inferSchemaSql(emptyTempTableName: string, query: string): string {
    return `
-- Infer schema of new query.
CREATE OR REPLACE TABLE ${emptyTempTableName} AS (
  SELECT * FROM (${query}) AS insertions LIMIT 0
);`;
  }

  private compareSchemasSql(
    database: string,
    schema: string,
    targetName: string,
    shortEmptyTableName: string
  ): string {
    return `
-- Compare schemas
DECLARE dataform_columns ARRAY<STRING>;
DECLARE temp_table_columns ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_added ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_removed ARRAY<STRING>;

SET dataform_columns = (
  SELECT IFNULL(ARRAY_AGG(DISTINCT column_name), [])
  FROM \`${database}.${schema}.INFORMATION_SCHEMA.COLUMNS\`
  WHERE table_name = '${targetName}'
);

SET temp_table_columns = (
  SELECT IFNULL(ARRAY_AGG(STRUCT(column_name, data_type)), [])
  FROM \`${database}.${schema}.INFORMATION_SCHEMA.COLUMNS\`
  WHERE table_name = '${shortEmptyTableName}'
);

SET columns_added = (
  SELECT IFNULL(ARRAY_AGG(column_info), [])
  FROM UNNEST(temp_table_columns) AS column_info
  WHERE column_info.column_name NOT IN UNNEST(dataform_columns)
);
SET columns_removed = (
  SELECT IFNULL(ARRAY_AGG(column_name), [])
  FROM UNNEST(dataform_columns) AS column_name
  WHERE column_name NOT IN (SELECT col.column_name FROM UNNEST(temp_table_columns) AS col)
);`;
  }

  private applySchemaChangeStrategySql(
    table: dataform.ITable,
    qualifiedTargetTableName: string
  ): string {
    const onSchemaChange = table.onSchemaChange || dataform.OnSchemaChange.IGNORE;
    let sql = `
-- Apply schema change strategy (${dataform.OnSchemaChange[onSchemaChange]}).`;

    switch (onSchemaChange) {
      case dataform.OnSchemaChange.FAIL:
        sql += `
IF ARRAY_LENGTH(columns_added) > 0 OR ARRAY_LENGTH(columns_removed) > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    "Schema mismatch defined by on_schema_change = 'FAIL'. Added columns: %t, removed columns: %t",
    columns_added,
    columns_removed
  );
END IF;
`;
        break;
      case dataform.OnSchemaChange.EXTEND:
        sql += `
IF ARRAY_LENGTH(columns_removed) > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    "Column removals are not allowed when on_schema_change = 'EXTEND'. Removed columns: %t",
    columns_removed
  );
END IF;

FOR column_info IN (SELECT * FROM UNNEST(columns_added)) DO
  EXECUTE IMMEDIATE FORMAT(
    "ALTER TABLE ${qualifiedTargetTableName} ADD COLUMN IF NOT EXISTS %s %s",
    column_info.column_name,
    column_info.data_type
  );
END FOR;
`;
        break;
      case dataform.OnSchemaChange.SYNCHRONIZE:
        const uniqueKeys = table.uniqueKey || [];
        sql += `
FOR removed_column_name IN (SELECT * FROM UNNEST(columns_removed)) DO
  IF removed_column_name IN UNNEST(${JSON.stringify(uniqueKeys)}) THEN
    RAISE USING MESSAGE = FORMAT(
      "Cannot drop column %s as it is part of the unique key for table ${qualifiedTargetTableName}",
      removed_column_name
    );
  ELSE
    EXECUTE IMMEDIATE FORMAT(
      "ALTER TABLE ${qualifiedTargetTableName} DROP COLUMN IF EXISTS %s",
      removed_column_name
    );
  END IF;
END FOR;

FOR column_info IN (SELECT * FROM UNNEST(columns_added)) DO
  EXECUTE IMMEDIATE FORMAT(
    "ALTER TABLE ${qualifiedTargetTableName} ADD COLUMN IF NOT EXISTS %s %s",
    column_info.column_name,
    column_info.data_type
  );
END FOR;
`;
        break;
    }
    return sql;
  }

  private runFinalDmlSql(
    table: dataform.ITable,
    qualifiedTargetTableName: string,
    dataTempTableName: string
  ): string {
    let finalDmlSql = "\n-- Run final MERGE/INSERT.";

    // Create temp table for incremental data.
    finalDmlSql += `
CREATE OR REPLACE TEMP TABLE ${dataTempTableName} AS (
  SELECT * FROM (${table.incrementalQuery || table.query})
);`;

    // Generate dynamic column lists from temp_table_columns.
    finalDmlSql += `
DECLARE dataform_columns_list STRING;
SET dataform_columns_list = (
  SELECT IFNULL(STRING_AGG(CONCAT('\`', column_name, '\`'), ', '), '')
  FROM UNNEST(temp_table_columns)
);`;

    // Run final MERGE/INSERT.
    if (table.uniqueKey && table.uniqueKey.length > 0) {
      const mergeOnClause = table.uniqueKey.map(k => `T.\`${k}\` = S.\`${k}\``).join(" and ");
      finalDmlSql += `
DECLARE dataform_columns_merge STRING;
SET dataform_columns_merge = (
  SELECT IFNULL(STRING_AGG(CONCAT('\`', column_name, '\` = S.\`', column_name, '\`'), ', '), '')
  FROM UNNEST(temp_table_columns)
);

IF ARRAY_LENGTH(temp_table_columns) > 0 THEN
  EXECUTE IMMEDIATE (
    "MERGE \`${qualifiedTargetTableName}\` T " ||
    "USING \`${dataTempTableName}\` S " ||
    "ON ${mergeOnClause} " ||
    "WHEN MATCHED THEN " ||
    "  UPDATE SET " || dataform_columns_merge || " " ||
    "WHEN NOT MATCHED THEN " ||
    "  INSERT (" || dataform_columns_list || ") VALUES (" || dataform_columns_list || ")"
  );
END IF;
`;
    } else {
      finalDmlSql += `
IF ARRAY_LENGTH(temp_table_columns) > 0 THEN
  EXECUTE IMMEDIATE (
    "INSERT INTO \`${qualifiedTargetTableName}\` (" || dataform_columns_list || ") " ||
    "SELECT " || dataform_columns_list || " FROM \`${dataTempTableName}\`"
  );
END IF;
`;
    }
    return finalDmlSql;
  }

  private cleanupSql(emptyTempTableName: string, dataTempTableName: string): string {
    return `
-- Cleanup temporary tables.
DROP TABLE IF EXISTS ${emptyTempTableName};
DROP TABLE IF EXISTS ${dataTempTableName};
    `;
  }

  private incrementalSchemaChangeBody(
    table: dataform.ITable,
    qualifiedTargetTableName: string,
    emptyTempTableName: string,
    dataTempTableName: string,
    shortEmptyTableName: string
  ): string {
    const statements: string[] = [
      this.inferSchemaSql(emptyTempTableName, table.incrementalQuery || table.query),
      this.compareSchemasSql(
        table.target.database,
        table.target.schema,
        table.target.name,
        shortEmptyTableName
      ),
      this.applySchemaChangeStrategySql(table, qualifiedTargetTableName),
      this.runFinalDmlSql(table, qualifiedTargetTableName, dataTempTableName),
      this.cleanupSql(emptyTempTableName, dataTempTableName)
    ];

    return statements.join("\n\n");
  }

  private createOrReplace(table: dataform.ITable) {
    const options = [];
    if (table.bigquery && table.bigquery.partitionBy && table.bigquery.partitionExpirationDays) {
      options.push(`partition_expiration_days=${table.bigquery.partitionExpirationDays}`);
    }
    if (table.bigquery && table.bigquery.partitionBy && table.bigquery.requirePartitionFilter) {
      options.push(`require_partition_filter=${table.bigquery.requirePartitionFilter}`);
    }
    if (table.bigquery && table.bigquery.additionalOptions) {
      for (const [optionName, optionValue] of Object.entries(table.bigquery.additionalOptions)) {
        options.push(`${optionName}=${optionValue}`);
      }
    }

    return `create or replace ${table.materialized ? "materialized " : ""}${this.tableTypeAsSql(
      this.baseTableType(table.enumType)
    )} ${this.resolveTarget(table.target)} ${
      table.bigquery && table.bigquery.partitionBy
        ? `partition by ${table.bigquery.partitionBy} `
        : ""
    }${
      table.bigquery && table.bigquery.clusterBy && table.bigquery.clusterBy.length > 0
        ? `cluster by ${table.bigquery.clusterBy.join(", ")} `
        : ""
    }${options.length > 0 ? `OPTIONS(${options.join(",")})` : ""}as ${table.query}`;
  }

  private createOrReplaceView(target: dataform.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  private mergeInto(
    target: dataform.ITarget,
    columns: string[],
    query: string,
    uniqueKey: string[],
    updatePartitionFilter: string
  ) {
    const backtickedColumns = columns.map(column => `\`${column}\``);
    return `
merge ${this.resolveTarget(target)} T
using (${query}
) S
on ${uniqueKey.map(uniqueKeyCol => `T.${uniqueKeyCol} = S.${uniqueKeyCol}`).join(` and `)}
  ${updatePartitionFilter ? `and T.${updatePartitionFilter}` : ""}
when matched then
  update set ${columns.map(column => `\`${column}\` = S.${column}`).join(",")}
when not matched then
  insert (${backtickedColumns.join(",")}) values (${backtickedColumns.join(",")})`;
  }
}

export function collectEvaluationQueries(
  queryOrAction: QueryOrAction,
  concatenate: boolean,
  queryModifier: (mod: string) => string = (q: string) => q
): IValidationQuery[] {
  // TODO: The prefix method (via `queryModifier`) is a bit sketchy. For example after
  // attaching the `explain` prefix, a table or operation could look like this:
  // ```
  // explain
  // -- Delete the temporary table, if it exists (perhaps from a previous run).
  // DROP TABLE IF EXISTS "df_integration_test"."load_from_s3_temp" CASCADE;
  // ```
  // which is invalid because the `explain` is interrupted by a comment.
  const validationQueries = new Array<IValidationQuery>();
  if (typeof queryOrAction === "string") {
    validationQueries.push({ query: queryModifier(queryOrAction) });
  } else {
    try {
      if (queryOrAction instanceof dataform.Table) {
        if (queryOrAction.enumType === dataform.TableType.INCREMENTAL) {
          const incrementalTableQueries = queryOrAction.incrementalPreOps.concat(
            queryOrAction.incrementalQuery,
            queryOrAction.incrementalPostOps
          );
          if (concatenate) {
            validationQueries.push({
              query: concatenateQueries(incrementalTableQueries, queryModifier),
              incremental: true
            });
          } else {
            incrementalTableQueries.forEach(q =>
              validationQueries.push({ query: queryModifier(q), incremental: true })
            );
          }
        }
        const tableQueries = queryOrAction.preOps.concat(
          queryOrAction.query,
          queryOrAction.postOps
        );
        if (concatenate) {
          validationQueries.push({
            query: concatenateQueries(tableQueries, queryModifier)
          });
        } else {
          tableQueries.forEach(q => validationQueries.push({ query: queryModifier(q) }));
        }
      } else if (queryOrAction instanceof dataform.Operation) {
        if (concatenate) {
          validationQueries.push({
            query: concatenateQueries(queryOrAction.queries, queryModifier)
          });
        } else {
          queryOrAction.queries.forEach(q => validationQueries.push({ query: queryModifier(q) }));
        }
      } else if (queryOrAction instanceof dataform.Assertion) {
        validationQueries.push({ query: queryModifier(queryOrAction.query) });
      } else {
        throw new Error("Unrecognized evaluate type.");
      }
    } catch (e) {
      throw new ErrorWithCause(`Error building tasks for evaluation. ${e.message}`, e);
    }
  }
  return validationQueries
    .map(validationQuery => ({ query: validationQuery.query.trim(), ...validationQuery }))
    .filter(validationQuery => !!validationQuery.query);
}
