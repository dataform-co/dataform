import * as semver from "semver";

import { Task, Tasks } from "df/cli/api/dbadapters/tasks";
import { concatenateQueries } from "df/cli/api/utils/query_transforms";
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
    private readonly dataformCoreVersion: string,
    private readonly uniqueIdGenerator: () => string = () =>
      Math.random().toString(36).substring(2),
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

  public insertInto(target: dataform.ITarget, columns: string[] | undefined, source: string) {
    const cols = columns
      ? columns.map((c) => `\`${c}\``).join(",")
      : '""" || dataform_columns_list || """';
    const resolvedTarget = columns
      ? this.resolveTarget(target)
      : this.escapeSqlString(this.resolveTarget(target));
    const dml = `	
insert into ${resolvedTarget}	
(${cols})	
select ${cols}	
from ${source}`;
    return columns ? dml : `EXECUTE IMMEDIATE ("""${dml}""");`;
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
    tableMetadata?: dataform.ITableMetadata,
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
    tableMetadata?: dataform.ITableMetadata,
  ): Task[] {
    let preOps = table.preOps;
    if (
      semver.gt(this.dataformCoreVersion, "1.4.8") &&
      table.enumType === dataform.TableType.INCREMENTAL &&
      this.shouldWriteIncrementally(table, runConfig, tableMetadata)
    ) {
      preOps = table.incrementalPreOps;
    }
    return (preOps || []).map((pre) => Task.statement(pre));
  }

  public postOps(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata,
  ): Task[] {
    let postOps = table.postOps;
    if (
      semver.gt(this.dataformCoreVersion, "1.4.8") &&
      table.enumType === dataform.TableType.INCREMENTAL &&
      this.shouldWriteIncrementally(table, runConfig, tableMetadata)
    ) {
      postOps = table.incrementalPostOps;
    }
    return (postOps || []).map((post) => Task.statement(post));
  }

  public resolveTarget(target: dataform.ITarget) {
    return this.CompilationSql.resolveTarget(target);
  }

  public getIncrementalQuery(table: dataform.ITable): string {
    return this.where(table.incrementalQuery || table.query, table.where);
  }

  public publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata,
  ): Tasks {
    const tasks = new Tasks();
    const preOpTasks = this.preOps(table, runConfig, tableMetadata);
    const postOpTasks = this.postOps(table, runConfig, tableMetadata);
    const isIncrementalSchemaChange =
      table.enumType === dataform.TableType.INCREMENTAL &&
      this.shouldWriteIncrementally(table, runConfig, tableMetadata) &&
      (table.onSchemaChange ?? dataform.OnSchemaChange.IGNORE) !== dataform.OnSchemaChange.IGNORE;

    if (!isIncrementalSchemaChange) {
      preOpTasks.forEach((statement) => tasks.add(statement));
    }

    const baseTableType = this.baseTableType(table.enumType);
    if (tableMetadata && tableMetadata.type !== baseTableType) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(baseTableType))),
      );
    }

    if (table.enumType === dataform.TableType.INCREMENTAL) {
      if (!this.shouldWriteIncrementally(table, runConfig, tableMetadata)) {
        tasks.add(Task.statement(this.createOrReplace(table)));
      } else {
        const onSchemaChange = table.onSchemaChange ?? dataform.OnSchemaChange.IGNORE;
        switch (onSchemaChange) {
          case dataform.OnSchemaChange.FAIL:
          case dataform.OnSchemaChange.EXTEND:
          case dataform.OnSchemaChange.SYNCHRONIZE:
            this.buildIncrementalSchemaChangeTasks(tasks, table, preOpTasks, postOpTasks);
            break;
          case dataform.OnSchemaChange.IGNORE:
            const columns = tableMetadata?.fields.map((f) => f.name) || [];
            tasks.add(Task.statement(this.getIncrementalDmlStatement(table, columns)));
            break;
        }
      }
    } else {
      tasks.add(Task.statement(this.createOrReplace(table)));
    }

    if (!isIncrementalSchemaChange) {
      postOpTasks.forEach((statement) => tasks.add(statement));
    }

    return tasks.concatenate();
  }

  public createTableTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata,
  ): dataform.IExecutionTask[] {
    return table.disabled ? [] : this.publishTasks(table, runConfig, tableMetadata).build();
  }

  public createOperationTasks(operation: dataform.IOperation): dataform.IExecutionTask[] {
    if (operation.disabled) {
      return [];
    }
    const queries = (operation.queries || []).filter((query) => !!query?.trim());
    if (queries.length === 0) {
      return [];
    }
    const statement = queries.length === 1 ? queries[0] : concatenateQueries(queries);
    return [dataform.ExecutionTask.create({ type: "statement", statement })];
  }

  public createPropertyGraphTasks(
    propertyGraph: dataform.IPropertyGraph,
  ): dataform.IExecutionTask[] {
    const statement =
      `CREATE OR REPLACE PROPERTY GRAPH ${this.resolveTarget(propertyGraph.target)} ` +
      `${propertyGraph.graphBody}`;
    return [dataform.ExecutionTask.create({ type: "statement", statement })];
  }

  public createAssertionTasks(assertion: dataform.IAssertion): dataform.IExecutionTask[] {
    return assertion.disabled ? [] : this.assertTasks(assertion, this.project).build();
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

  private buildIncrementalPredicatesString(incrementalPredicates?: string[] | null): string {
    const validPredicates = incrementalPredicates
      ? incrementalPredicates.filter((p) => p.trim() !== "")
      : [];
    if (validPredicates.length === 0) {
      return "";
    }
    return `and ${validPredicates.map((p) => `(${p})`).join(" and ")}`;
  }

  private isMergeStrategy(table: dataform.ITable): boolean {
    return (
      table.incrementalStrategy !== dataform.IncrementalStrategy.INSERT_OVERWRITE &&
      Boolean(table.uniqueKey && table.uniqueKey.length > 0)
    );
  }

  private buildIncrementalSchemaChangeTasks(
    tasks: Tasks,
    table: dataform.ITable,
    preOpTasks: Task[] = [],
    postOpTasks: Task[] = [],
  ) {
    const uniqueId = this.uniqueIdGenerator();

    const emptyTempTableTarget = {
      ...table.target,
      name: `${table.target.name}_df_temp_${uniqueId}_empty`,
    };

    const procedureName = this.createProcedureName(table.target, uniqueId);
    const procedureBody = this.incrementalSchemaChangeBody(
      table,
      this.resolveTarget(table.target),
      emptyTempTableTarget,
      preOpTasks,
      postOpTasks,
    );

    const createProcedureSql = `CREATE OR REPLACE PROCEDURE ${procedureName}()
OPTIONS(strict_mode=false)
BEGIN
${procedureBody}
END;`;

    const callProcedureSql = this.safeCallAndDropProcedure(
      procedureName,
      this.resolveTarget(emptyTempTableTarget),
    );
    tasks.add(Task.statement(createProcedureSql));
    tasks.add(Task.statement(callProcedureSql));
  }

  private createProcedureName(target: dataform.ITarget, uniqueId: string): string {
    return this.resolveTarget({
      ...target,
      name: `df_osc_${uniqueId}`,
    });
  }

  private safeCallAndDropProcedure(procedureName: string, emptyTempTableName: string): string {
    return `
BEGIN
  CALL ${procedureName}();
EXCEPTION WHEN ERROR THEN
  DROP TABLE IF EXISTS ${emptyTempTableName};
  DROP PROCEDURE IF EXISTS ${procedureName};
  RAISE;
END;
DROP PROCEDURE IF EXISTS ${procedureName};`;
  }

  private declareSchemaChangeVariablesSql(table: dataform.ITable): string {
    const onSchemaChange = table.onSchemaChange || dataform.OnSchemaChange.IGNORE;

    let sql = `
-- Declare variables for schema comparison and strategy execution.
DECLARE dataform_columns ARRAY<STRING>;
DECLARE dataform_columns_list STRING;`;

    if (this.isMergeStrategy(table)) {
      sql += `\nDECLARE dataform_columns_merge STRING;`;
    }

    sql += `
DECLARE temp_table_columns ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_added ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_removed ARRAY<STRING>;`;

    if (onSchemaChange === dataform.OnSchemaChange.SYNCHRONIZE) {
      sql += `\nDECLARE invalid_removed_columns ARRAY<STRING>;`;
    }
    return sql;
  }

  private createEmptyTempTableSql(emptyTempTableName: string, query: string): string {
    return `
-- Create empty table to extract schema of new query.
CREATE OR REPLACE TABLE ${emptyTempTableName} AS (
  SELECT * FROM (${query}) AS insertions LIMIT 0
);`;
  }

  private compareSchemasSql(
    target: dataform.ITarget,
    emptyTempTableTarget: dataform.ITarget,
  ): string {
    return `
-- Compare schemas
SET dataform_columns = (
  SELECT IFNULL(ARRAY_AGG(DISTINCT column_name), [])
  FROM \`${target.database}.${target.schema}.INFORMATION_SCHEMA.COLUMNS\`
  WHERE table_name = '${target.name}'
);

SET temp_table_columns = (
  SELECT IFNULL(ARRAY_AGG(STRUCT(column_name, data_type)), [])
  FROM \`${emptyTempTableTarget.database}.${emptyTempTableTarget.schema}.INFORMATION_SCHEMA.COLUMNS\`
  WHERE table_name = '${emptyTempTableTarget.name}'
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
    qualifiedTargetTableName: string,
  ): string {
    const onSchemaChange = table.onSchemaChange || dataform.OnSchemaChange.IGNORE;
    let sql = `
-- Apply schema change strategy (${dataform.OnSchemaChange[onSchemaChange]}).`;

    switch (onSchemaChange) {
      case dataform.OnSchemaChange.FAIL:
        sql += `
IF ARRAY_LENGTH(columns_added) > 0 OR ARRAY_LENGTH(columns_removed) > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    "Schema mismatch defined by on_schema_change = 'FAIL'. Added columns: %T, removed columns: %T",
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
    "Column removals are not allowed when on_schema_change = 'EXTEND'. Removed columns: %T",
    columns_removed
  );
END IF;

${this.alterTableAddColumnsSql(qualifiedTargetTableName)}
`;
        break;
      case dataform.OnSchemaChange.SYNCHRONIZE:
        const uniqueKeys = table.uniqueKey || [];
        sql += `
SET invalid_removed_columns = (
  SELECT IFNULL(ARRAY_AGG(col), []) FROM UNNEST(columns_removed) AS col WHERE col IN UNNEST(${JSON.stringify(uniqueKeys)})
);

IF ARRAY_LENGTH(invalid_removed_columns) > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    "Cannot drop columns %T as they are part of the unique key for table ${qualifiedTargetTableName}",
    invalid_removed_columns
  );
END IF;

IF ARRAY_LENGTH(columns_removed) > 0 THEN
  EXECUTE IMMEDIATE (
    "ALTER TABLE ${qualifiedTargetTableName} " ||
    (
      SELECT STRING_AGG(FORMAT("DROP COLUMN IF EXISTS %s", col), ", ")
      FROM UNNEST(columns_removed) AS col
    )
  );
END IF;

${this.alterTableAddColumnsSql(qualifiedTargetTableName)}
`;
        break;
    }
    return sql;
  }

  private alterTableAddColumnsSql(qualifiedTargetTableName: string): string {
    return `IF ARRAY_LENGTH(columns_added) > 0 THEN
  EXECUTE IMMEDIATE (
    "ALTER TABLE ${qualifiedTargetTableName} " ||
    (
      SELECT STRING_AGG(FORMAT("ADD COLUMN IF NOT EXISTS %s %s", column_info.column_name, column_info.data_type), ", ")
      FROM UNNEST(columns_added) AS column_info
    )
  );
END IF;`;
  }

  private prepareDynamicColumnsSql(table: dataform.ITable): string {
    let sql = `
-- Prepare dynamic column lists for DML.
SET dataform_columns_list = (
  SELECT STRING_AGG(FORMAT("\`%s\`", column_info.column_name), ", ")
  FROM UNNEST(temp_table_columns) AS column_info
);`;

    if (this.isMergeStrategy(table)) {
      sql += `
SET dataform_columns_merge = (
  SELECT STRING_AGG(FORMAT("\`%s\` = DATAFORM_SOURCE.\`%s\`", column_info.column_name, column_info.column_name), ", ")
  FROM UNNEST(temp_table_columns) AS column_info
);`;
    }

    return sql;
  }

  private cleanupSql(emptyTempTableName: string): string {
    return `
-- Cleanup empty temp table
DROP TABLE IF EXISTS ${emptyTempTableName};
    `;
  }

  private terminateStatements(statements: string[]): string {
    const joined = concatenateQueries([...statements, "DUMMY"]);
    return joined === "DUMMY" ? "" : joined.slice(0, -"\nDUMMY".length);
  }

  private incrementalSchemaChangeBody(
    table: dataform.ITable,
    qualifiedTargetTableName: string,
    emptyTempTableTarget: dataform.ITarget,
    preOpTasks: Task[] = [],
    postOpTasks: Task[] = [],
  ): string {
    const emptyTempTableName = this.resolveTarget(emptyTempTableTarget);
    const query = this.getIncrementalQuery(table);
    const coreBody = [
      this.declareSchemaChangeVariablesSql(table),
      this.createEmptyTempTableSql(emptyTempTableName, query),
      this.compareSchemasSql(table.target, emptyTempTableTarget),
      this.applySchemaChangeStrategySql(table, qualifiedTargetTableName),
      this.prepareDynamicColumnsSql(table),
      this.getIncrementalDmlStatement(table),
      this.cleanupSql(emptyTempTableName),
    ].join("\n\n");

    const preOpsSql = this.terminateStatements(preOpTasks.map((t) => t.getStatement()));
    const postOpsSql = this.terminateStatements(postOpTasks.map((t) => t.getStatement()));

    const bodyParts: string[] = [];
    if (preOpsSql) {
      bodyParts.push(preOpsSql);
      bodyParts.push(`BEGIN\n${coreBody}\nEND;`);
    } else {
      bodyParts.push(coreBody);
    }
    if (postOpsSql) {
      bodyParts.push(postOpsSql);
    }

    return bodyParts.join("\n\n");
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
      this.baseTableType(table.enumType),
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

  private escapeSqlString(str: string): string {
    return str.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  }

  private withTempStagingTable(query: string, buildBody: (stagingTable: string) => string): string {
    const uniqueId = this.uniqueIdGenerator();
    const stagingTable = `\`staging_table_temp_${uniqueId}\``;
    return `CREATE OR REPLACE TEMP TABLE ${stagingTable} AS (
  ${query}
);

${buildBody(stagingTable)}

-- Cleanup staging table
DROP TABLE IF EXISTS ${stagingTable};`;
  }

  private mergeInto(
    target: dataform.ITarget,
    columns: string[] | undefined,
    source: string,
    uniqueKey: string[],
    bigquery: dataform.IBigQueryOptions,
  ) {
    const cols = columns
      ? columns.map((column) => `\`${column}\``).join(",")
      : '""" || dataform_columns_list || """';
    const updateSet = columns
      ? columns.map((column) => `\`${column}\` = DATAFORM_SOURCE.${column}`).join(",")
      : '""" || dataform_columns_merge || """';
    const updatePartitionFilter = bigquery && bigquery.updatePartitionFilter;
    const incrementalPredicates = bigquery && bigquery.incrementalPredicates;
    const incrementalPredicatesString =
      this.buildIncrementalPredicatesString(incrementalPredicates);
    const resolvedTarget = columns
      ? this.resolveTarget(target)
      : this.escapeSqlString(this.resolveTarget(target));
    const onCondition = `${uniqueKey.map((uniqueKeyCol) => `DATAFORM_DEST.${uniqueKeyCol} = DATAFORM_SOURCE.${uniqueKeyCol}`).join(` and `)} ${updatePartitionFilter ? `and DATAFORM_DEST.${updatePartitionFilter}` : ""}\n${incrementalPredicatesString ? ` ${incrementalPredicatesString}` : ""}`;

    const dml = `
merge ${resolvedTarget} DATAFORM_DEST
using ${source} DATAFORM_SOURCE
on ${columns ? onCondition : this.escapeSqlString(onCondition)}
when matched then
  update set ${updateSet}
when not matched then
  insert (${cols}) values (${cols})`;

    return columns ? dml : `EXECUTE IMMEDIATE ("""${dml}""");`;
  }

  private insertOverwrite(
    target: dataform.ITarget,
    columns: string[] | undefined,
    stagingTable: string,
    bigquery: dataform.IBigQueryOptions,
  ): string {
    const cols = columns
      ? columns.map((column) => `\`${column}\``).join(",")
      : '""" || dataform_columns_list || """';
    const partitionBy = bigquery && bigquery.partitionBy;
    const updatePartitionFilter = bigquery && bigquery.updatePartitionFilter;
    const incrementalPredicates = bigquery && bigquery.incrementalPredicates;
    const incrementalPredicatesString =
      this.buildIncrementalPredicatesString(incrementalPredicates);
    const resolveTargetTable = columns
      ? this.resolveTarget(target)
      : this.escapeSqlString(this.resolveTarget(target));
    const partitionsVar = columns ? "partitions_for_replacement" : "@partitions_for_replacement";
    const notMatchedBySourceCondition = `${partitionBy} IN UNNEST(${partitionsVar}) ${updatePartitionFilter ? `and DATAFORM_DEST.${updatePartitionFilter}` : ""}\n  ${incrementalPredicatesString ? ` ${incrementalPredicatesString}` : ""}`;

    const mergeSql = `MERGE ${resolveTargetTable} DATAFORM_DEST
  USING ${stagingTable} DATAFORM_SOURCE
  ON FALSE
  WHEN NOT MATCHED BY SOURCE AND ${columns ? notMatchedBySourceCondition : this.escapeSqlString(notMatchedBySourceCondition)}
  THEN
    DELETE
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (${cols}) VALUES (${cols})`;

    return `BEGIN
  DECLARE partitions_for_replacement DEFAULT (
    ARRAY(
      SELECT DISTINCT ${partitionBy}
      FROM ${stagingTable}
      WHERE ${partitionBy} IS NOT NULL
    )
  );

  ${columns ? mergeSql : `EXECUTE IMMEDIATE ("""${mergeSql}""")\n  USING partitions_for_replacement AS partitions_for_replacement`};
END;`;
  }

  private getIncrementalDmlStatement(table: dataform.ITable, columns?: string[]): string {
    const incrementalQuery = this.getIncrementalQuery(table);
    const isDynamic = !columns;

    switch (table.incrementalStrategy) {
      case dataform.IncrementalStrategy.INSERT_OVERWRITE:
        return this.withTempStagingTable(incrementalQuery, (stagingTable) =>
          this.insertOverwrite(table.target, columns, stagingTable, table.bigquery),
        );
      case dataform.IncrementalStrategy.MERGE:
      default:
        if (this.isMergeStrategy(table)) {
          return isDynamic
            ? this.withTempStagingTable(incrementalQuery, (stagingTable) =>
                this.mergeInto(
                  table.target,
                  undefined,
                  stagingTable,
                  table.uniqueKey,
                  table.bigquery,
                ),
              )
            : this.mergeInto(
                table.target,
                columns,
                `(${incrementalQuery}\n)`,
                table.uniqueKey,
                table.bigquery,
              );
        }
        return isDynamic
          ? this.withTempStagingTable(incrementalQuery, (stagingTable) =>
              this.insertInto(table.target, undefined, stagingTable),
            )
          : this.insertInto(table.target, columns, `(${incrementalQuery}) as insertions`);
    }
  }
}

export function collectEvaluationQueries(
  queryOrAction: QueryOrAction,
  concatenate: boolean,
  queryModifier: (mod: string) => string = (q: string) => q,
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
            queryOrAction.incrementalPostOps,
          );
          if (concatenate) {
            validationQueries.push({
              query: concatenateQueries(incrementalTableQueries, queryModifier),
              incremental: true,
            });
          } else {
            incrementalTableQueries.forEach((q) =>
              validationQueries.push({ query: queryModifier(q), incremental: true }),
            );
          }
        }
        const tableQueries = queryOrAction.preOps.concat(
          queryOrAction.query,
          queryOrAction.postOps,
        );
        if (concatenate) {
          validationQueries.push({
            query: concatenateQueries(tableQueries, queryModifier),
          });
        } else {
          tableQueries.forEach((q) => validationQueries.push({ query: queryModifier(q) }));
        }
      } else if (queryOrAction instanceof dataform.Operation) {
        if (concatenate) {
          validationQueries.push({
            query: concatenateQueries(queryOrAction.queries, queryModifier),
          });
        } else {
          queryOrAction.queries.forEach((q) => validationQueries.push({ query: queryModifier(q) }));
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
    .map((validationQuery) => ({ query: validationQuery.query.trim(), ...validationQuery }))
    .filter((validationQuery) => !!validationQuery.query);
}
