import * as semver from "semver";

import { IAdapter } from "df/core/adapters";
import { Task, Tasks } from "df/core/tasks";
import { dataform } from "df/protos/ts";

export abstract class Adapter implements IAdapter {
  constructor(protected readonly dataformCoreVersion: string) {}

  public abstract publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks;
  public abstract assertTasks(
    assertion: dataform.IAssertion,
    projectConfig: dataform.IProjectConfig
  ): Tasks;

  public abstract resolveTarget(target: dataform.ITarget): string;

  public normalizeIdentifier(identifier: string) {
    return identifier;
  }

  public sqlString(stringContents: string) {
    // Escape escape characters, then escape single quotes, then wrap the string in single quotes.
    return `'${stringContents.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
  }

  public dropIfExists(target: dataform.ITarget, type: dataform.TableMetadata.Type) {
    return `drop ${this.tableTypeAsSql(type)} if exists ${this.resolveTarget(target)} ${
      type === dataform.TableMetadata.Type.TABLE ? "cascade" : ""
    }`;
  }

  public baseTableType(type: string) {
    switch (type) {
      case "table":
      case "incremental":
        return dataform.TableMetadata.Type.TABLE;
      case "view":
        return dataform.TableMetadata.Type.VIEW;
      default:
        throw new Error(`Unexpected table type: ${type}`);
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

  protected insertInto(target: dataform.ITarget, columns: string[], query: string) {
    return `	
insert into ${this.resolveTarget(target)}	
(${columns.join(",")})	
select ${columns.join(",")}	
from (${query}) as insertions`;
  }

  protected oppositeTableType(type: dataform.TableMetadata.Type) {
    switch (type) {
      case dataform.TableMetadata.Type.TABLE:
        return dataform.TableMetadata.Type.VIEW;
      case dataform.TableMetadata.Type.VIEW:
        return dataform.TableMetadata.Type.TABLE;
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
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata
  ) {
    return (
      !runConfig.fullRefresh &&
      tableMetadata &&
      tableMetadata.type !== dataform.TableMetadata.Type.VIEW
    );
  }

  protected preOps(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata
  ): Task[] {
    let preOps = table.preOps;
    if (
      semver.gt(this.dataformCoreVersion, "1.4.8") &&
      table.type === "incremental" &&
      this.shouldWriteIncrementally(runConfig, tableMetadata)
    ) {
      preOps = table.incrementalPreOps;
    }
    return (preOps || []).map(pre => Task.statement(pre));
  }

  protected postOps(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata?: dataform.ITableMetadata
  ): Task[] {
    let postOps = table.postOps;
    if (
      semver.gt(this.dataformCoreVersion, "1.4.8") &&
      table.type === "incremental" &&
      this.shouldWriteIncrementally(runConfig, tableMetadata)
    ) {
      postOps = table.incrementalPostOps;
    }
    return (postOps || []).map(post => Task.statement(post));
  }
}
