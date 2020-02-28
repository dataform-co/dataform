import { Task, Tasks } from "@dataform/core/tasks";
import { dataform } from "@dataform/protos";
import * as semver from "semver";

export abstract class Adapter {
  constructor(protected readonly dataformCoreVersion: string) {}

  public abstract resolveTarget(target: dataform.ITarget): string;

  public normalizeIdentifier(identifier: string) {
    return identifier;
  }

  public sqlString(stringContents: string) {
    // Escape escape characters, then escape single quotes, then wrap the string in single quotes.
    return `'${stringContents.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)} ${
      this.baseTableType(type) === "table" ? "cascade" : ""
    }`;
  }

  public baseTableType(type: string) {
    if (type === "incremental") {
      return "table";
    }
    return type;
  }

  protected insertInto(target: dataform.ITarget, columns: string[], query: string) {
    return `
insert into ${this.resolveTarget(target)}
(${columns.join(",")})
select ${columns.join(",")}
from (${query}) as insertions`;
  }

  protected oppositeTableType(type: string) {
    return this.baseTableType(type) === "table" ? "view" : "table";
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
    return !runConfig.fullRefresh && tableMetadata && tableMetadata.type !== "view";
  }

  protected preOps(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
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
    tableMetadata: dataform.ITableMetadata
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
