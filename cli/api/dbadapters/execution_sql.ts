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
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata
  ) {
    return (
      !runConfig.fullRefresh &&
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
      this.shouldWriteIncrementally(runConfig, tableMetadata)
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
      this.shouldWriteIncrementally(runConfig, tableMetadata)
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
      if (!this.shouldWriteIncrementally(runConfig, tableMetadata)) {
        tasks.add(Task.statement(this.createOrReplace(table)));
      } else {
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
      }
    } else {
      tasks.add(Task.statement(this.createOrReplace(table)));
    }

    this.postOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    return tasks.concatenate();
  }

  public assertTasks(
    assertion: dataform.IAssertion,
    projectConfig: dataform.IProjectConfig
  ): Tasks {
    const tasks = new Tasks();
    const target = assertion.target;
    tasks.add(Task.statement(this.createOrReplaceView(target, assertion.query)));
    tasks.add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
    return tasks;
  }

  public dropIfExists(target: dataform.ITarget, type: dataform.TableMetadata.Type) {
    return `drop ${this.tableTypeAsSql(type)} if exists ${this.resolveTarget(target)}`;
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
