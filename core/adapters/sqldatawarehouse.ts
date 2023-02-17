import { IAdapter } from "df/core/adapters";
import { Adapter } from "df/core/adapters/base";
import { Task, Tasks } from "df/core/tasks";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export class SQLDataWarehouseAdapter extends Adapter implements IAdapter {
  constructor(private readonly project: core.ProjectConfig, dataformCoreVersion: string) {
    super(dataformCoreVersion);
  }

  public sqlString(stringContents: string) {
    // Double single quotes (effectively escaping them), then wrap the string in single quotes.
    return `'${stringContents.replace(/'/g, "''")}'`;
  }

  public resolveTarget(target: core.Target) {
    return `"${target.schema}"."${target.name}"`;
  }

  public publishTasks(
    table: core.Table,
    runConfig: execution.RunConfig,
    tableMetadata: execution.TableMetadata
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
      if (!this.shouldWriteIncrementally(runConfig, tableMetadata)) {
        tasks.addAll(this.createOrReplace(table, !!tableMetadata));
      } else {
        tasks.add(
          Task.statement(
            this.insertInto(
              table.target,
              tableMetadata.fields.map(f => f.name),
              this.where(table.incrementalQuery || table.query, table.where)
            )
          )
        );
      }
    } else {
      tasks.addAll(this.createOrReplace(table, !!tableMetadata));
    }

    this.postOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    return tasks.concatenate();
  }

  public assertTasks(assertion: core.Assertion, projectConfig: core.ProjectConfig): Tasks {
    const target = assertion.target;
    return Tasks.create()
      .add(Task.statement(this.dropIfExists(target, execution.TableMetadata_Type.VIEW)))
      .add(
        Task.statement(`
        create view ${this.resolveTarget(target)}
        as ${assertion.query}`)
      )
      .add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
  }

  public dropIfExists(target: core.Target, type: execution.TableMetadata_Type) {
    if (type === execution.TableMetadata_Type.VIEW) {
      return `drop view if exists ${this.resolveTarget(target)} `;
    }
    return `if object_id ('${this.resolveTarget(
      target
    )}','U') is not null drop table ${this.resolveTarget(target)}`;
  }

  public insertInto(target: core.Target, columns: string[], query: string) {
    return `
insert into ${this.resolveTarget(target)}
(${columns.join(",")})
select ${columns.join(",")}
from (${query}
) as insertions`;
  }

  private createOrReplace(table: core.Table, alreadyExists: boolean) {
    if (table.enumType === core.TableType.VIEW) {
      return Tasks.create().add(
        Task.statement(
          `${alreadyExists ? "alter" : "create"} view ${this.resolveTarget(table.target)} as ${
            table.query
          }`
        )
      );
    }
    const tempTableTarget = core.Target.create({
      schema: table.target.schema,
      name: table.target.name + "_temp"
    });

    return Tasks.create()
      .add(Task.statement(this.dropIfExists(tempTableTarget, this.baseTableType(table.enumType))))
      .add(Task.statement(this.createTable(table, tempTableTarget)))
      .add(Task.statement(this.dropIfExists(table.target, execution.TableMetadata_Type.TABLE)))
      .add(
        Task.statement(
          `rename object ${this.resolveTarget(tempTableTarget)} to ${table.target.name} `
        )
      );
  }

  private createTable(table: core.Table, target: core.Target) {
    const distribution =
      table.sqlDataWarehouse && table.sqlDataWarehouse.distribution
        ? table.sqlDataWarehouse.distribution
        : "ROUND_ROBIN"; // default
    return `create table ${this.resolveTarget(target)}
     with(
       distribution = ${distribution}
     ) 
     as ${table.query}`;
  }
}
