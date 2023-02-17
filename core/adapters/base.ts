import * as semver from "semver";

import { IAdapter } from "df/core/adapters";
import { Task, Tasks } from "df/core/tasks";
import { tableTypeEnumToString } from "df/core/utils";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export abstract class Adapter implements IAdapter {
  constructor(protected readonly coreCoreVersion: string) {}

  public abstract publishTasks(
    table: core.Table,
    runConfig: execution.RunConfig,
    tableMetadata: execution.TableMetadata
  ): Tasks;
  public abstract assertTasks(assertion: core.Assertion, projectConfig: core.ProjectConfig): Tasks;

  public abstract resolveTarget(target: core.Target): string;

  public normalizeIdentifier(identifier: string) {
    return identifier;
  }

  public sqlString(stringContents: string) {
    // Escape escape characters, then escape single quotes, then wrap the string in single quotes.
    return `'${stringContents.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
  }

  public dropIfExists(target: core.Target, type: execution.TableMetadata_Type) {
    return `drop ${this.tableTypeAsSql(type)} if exists ${this.resolveTarget(target)} ${
      type === execution.TableMetadata_Type.TABLE ? "cascade" : ""
    }`;
  }

  public baseTableType(enumType: core.TableType) {
    switch (enumType) {
      case core.TableType.TABLE:
      case core.TableType.INCREMENTAL:
        return execution.TableMetadata_Type.TABLE;
      case core.TableType.VIEW:
        return execution.TableMetadata_Type.VIEW;
      default:
        throw new Error(`Unexpected table type: ${tableTypeEnumToString(enumType)}`);
    }
  }

  public tableTypeAsSql(type: execution.TableMetadata_Type) {
    switch (type) {
      case execution.TableMetadata_Type.TABLE:
        return "table";
      case execution.TableMetadata_Type.VIEW:
        return "view";
      default:
        throw new Error(`Unexpected table type: ${type}`);
    }
  }

  public indexAssertion(dataset: string, indexCols: string[]) {
    const commaSeparatedColumns = indexCols.join(", ");
    return `
SELECT
  *
FROM (
  SELECT
    ${commaSeparatedColumns},
    COUNT(1) AS index_row_count
  FROM ${dataset}
  GROUP BY ${commaSeparatedColumns}
  ) AS data
WHERE index_row_count > 1
`;
  }

  public rowConditionsAssertion(dataset: string, rowConditions: string[]) {
    return rowConditions
      .map(
        (rowCondition: string) => `
SELECT
  ${this.sqlString(rowCondition)} AS failing_row_condition,
  *
FROM ${dataset}
WHERE NOT (${rowCondition})
`
      )
      .join(`UNION ALL`);
  }

  protected insertInto(target: core.Target, columns: string[], query: string) {
    return `	
insert into ${this.resolveTarget(target)}	
(${columns.join(",")})	
select ${columns.join(",")}	
from (${query}) as insertions`;
  }

  protected oppositeTableType(type: execution.TableMetadata_Type) {
    switch (type) {
      case execution.TableMetadata_Type.TABLE:
        return execution.TableMetadata_Type.VIEW;
      case execution.TableMetadata_Type.VIEW:
        return execution.TableMetadata_Type.TABLE;
      default:
        throw new Error(`Unexpected table type: ${type}`);
    }
  }

  protected where(query: string, where: string) {
    return where
      ? `
  select * from (${query}) as subquery
    where ${where}`
      : query;
  }

  protected shouldWriteIncrementally(
    runConfig: execution.RunConfig,
    tableMetadata?: execution.TableMetadata
  ) {
    return (
      !runConfig.fullRefresh &&
      tableMetadata &&
      tableMetadata.type !== execution.TableMetadata_Type.VIEW
    );
  }

  protected preOps(
    table: core.Table,
    runConfig: execution.RunConfig,
    tableMetadata?: execution.TableMetadata
  ): Task[] {
    let preOps = table.preOps;
    if (
      semver.gt(this.coreCoreVersion, "1.4.8") &&
      table.enumType === core.TableType.INCREMENTAL &&
      this.shouldWriteIncrementally(runConfig, tableMetadata)
    ) {
      preOps = table.incrementalPreOps;
    }
    return (preOps || []).map(pre => Task.statement(pre));
  }

  protected postOps(
    table: core.Table,
    runConfig: execution.RunConfig,
    tableMetadata?: execution.TableMetadata
  ): Task[] {
    let postOps = table.postOps;
    if (
      semver.gt(this.coreCoreVersion, "1.4.8") &&
      table.enumType === core.TableType.INCREMENTAL &&
      this.shouldWriteIncrementally(runConfig, tableMetadata)
    ) {
      postOps = table.incrementalPostOps;
    }
    return (postOps || []).map(post => Task.statement(post));
  }
}
