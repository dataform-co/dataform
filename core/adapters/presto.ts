import { IAdapter } from "df/core/adapters";
import { Adapter } from "df/core/adapters/base";
import { Task, Tasks } from "df/core/tasks";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export class PrestoAdapter extends Adapter implements IAdapter {
  constructor(private readonly project: core.ProjectConfig, dataformCoreVersion: string) {
    super(dataformCoreVersion);
  }

  public resolveTarget(target: core.Target) {
    // Database here is equivalent to Presto's catalog.
    return `"${target.database || this.project.defaultDatabase}"."${target.schema ||
      this.project.defaultSchema}"."${target.name}"`;
  }

  public publishTasks(
    table: core.Table,
    runConfig: execution.RunConfig,
    tableMetadata?: execution.TableMetadata
  ): Tasks {
    const tasks = Tasks.create();

    this.preOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    const baseTableType = this.baseTableType(table.enumType);
    if (tableMetadata && tableMetadata.type !== baseTableType) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(baseTableType)))
      );
    }

    if (table.enumType === core.TableType.INCREMENTAL) {
      throw new Error("Incremental table types are not currently supported for Presto.");
    } else {
      tasks.add(Task.statement(this.createOrReplace(table)));
    }

    this.postOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    return tasks.concatenate();
  }

  public assertTasks(assertion: core.Assertion, projectConfig: core.ProjectConfig): Tasks {
    const tasks = Tasks.create();
    const target = assertion.target;
    tasks.add(Task.statement(this.createOrReplaceView(target, assertion.query)));
    tasks.add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
    return tasks;
  }

  public createOrReplaceView(target: core.Target, query: string) {
    return `create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  public createTable(table: core.Table) {
    return `create table if not exists ${this.resolveTarget(table.target)} as ${table.query} ${
      table.presto?.partitionBy
        ? `with (partitioned_by = array[${table.presto?.partitionBy.join(", ")}]`
        : ""
    }`;
  }

  public dropTableIfExists(target: core.Target, type: execution.TableMetadata_Type) {
    return `drop ${this.tableTypeAsSql(type)} if exists ${this.resolveTarget(target)}`;
  }

  public createOrReplace(table: core.Table) {
    // TODO: Not all connectors support replacing/dropping tables, but DROP is supported.
    // A solution should be thought about for handling presto connections to see whether the target
    // catalog allows dropping. For now default to creating the table.
    return this.createTable(table);
  }

  public mergeInto() {
    throw new Error("mergeInto unimplemented for Presto.");
    // MERGE isn't supported by Presto. Instead a solution could be formed by using a temporary
    // table (as we do in Redshift) using an in memory connection, as suggested here?
    // https://groups.google.com/g/presto-users/c/5I480n_1nPc
  }
}
