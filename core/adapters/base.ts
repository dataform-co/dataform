import { Task, Tasks } from "@dataform/core/tasks";
import { dataform } from "@dataform/protos";

export abstract class Adapter {
  public abstract resolveTarget(target: dataform.ITarget): string;

  public normalizeIdentifier(identifier: string) {
    return identifier;
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)} ${
      this.baseTableType(type) == "table" ? "cascade" : ""
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

  protected addPreOps(
    table: dataform.ITable,
    dataformCoreVersion: string,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Task[] {
    return (dataformCoreVersion > "1.4.8" &&
    table.type === "incremental" &&
    this.shouldWriteIncrementally(runConfig, tableMetadata)
      ? table.incrementalPreOps || []
      : table.preOps || []
    ).map(pre => Task.statement(pre));
  }

  protected addPostOps(
    table: dataform.ITable,
    dataformCoreVersion: string,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Task[] {
    return (dataformCoreVersion > "1.4.8" &&
    table.type === "incremental" &&
    this.shouldWriteIncrementally(runConfig, tableMetadata)
      ? table.incrementalPostOps || []
      : table.postOps || []
    ).map(post => Task.statement(post));
  }
}
