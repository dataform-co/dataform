import { IAdapter } from "df/core/adapters";
import { Adapter } from "df/core/adapters/base";
import { Task, Tasks } from "df/core/tasks";
import { dataform } from "df/protos/ts";

export class SQLDataWarehouseAdapter extends Adapter implements IAdapter {
  constructor(private readonly project: dataform.IProjectConfig, dataformCoreVersion: string) {
    super(dataformCoreVersion);
  }

  public sqlString(stringContents: string) {
    // Double single quotes (effectively escaping them), then wrap the string in single quotes.
    return `'${stringContents.replace(/'/g, "''")}'`;
  }

  public resolveTarget(target: dataform.ITarget) {
    return `"${target.schema}"."${target.name}"`;
  }

  public publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks {
    const tasks = Tasks.create();

    this.preOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    const baseTableType = this.baseTableType(table.type);
    if (tableMetadata && tableMetadata.type !== baseTableType) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(baseTableType)))
      );
    }

    if (table.type === "incremental") {
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

    if (runConfig.useSingleQueryPerAction) {
      return tasks.concatenate();
    }

    return tasks;
  }

  public assertTasks(assertion: dataform.IAssertion): Tasks {
    const target = assertion.target;

    return Tasks.create()
      .add(Task.statement(this.dropIfExists(target, dataform.TableMetadata.Type.VIEW)))
      .add(
        Task.statement(`
        create view ${this.resolveTarget(target)}
        as ${assertion.query}`)
      )
      .add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
  }

  public dropIfExists(target: dataform.ITarget, type: dataform.TableMetadata.Type) {
    if (type === dataform.TableMetadata.Type.VIEW) {
      return `drop view if exists ${this.resolveTarget(target)} `;
    }
    return `if object_id ('${this.resolveTarget(
      target
    )}','U') is not null drop table ${this.resolveTarget(target)}`;
  }

  public createOrReplace(table: dataform.ITable, alreadyExists: boolean) {
    if (table.type === "view") {
      return Tasks.create().add(
        Task.statement(
          `${alreadyExists ? "alter" : "create"} view ${this.resolveTarget(table.target)} as ${
            table.query
          }`
        )
      );
    }
    const tempTableTarget = dataform.Target.create({
      schema: table.target.schema,
      name: table.target.name + "_temp"
    });

    return Tasks.create()
      .add(Task.statement(this.dropIfExists(tempTableTarget, this.baseTableType(table.type))))
      .add(Task.statement(this.createTable(table, tempTableTarget)))
      .add(Task.statement(this.dropIfExists(table.target, dataform.TableMetadata.Type.TABLE)))
      .add(
        Task.statement(
          `rename object ${this.resolveTarget(tempTableTarget)} to ${table.target.name} `
        )
      );
  }

  public createTable(table: dataform.ITable, target: dataform.ITarget) {
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

  public insertInto(target: dataform.ITarget, columns: string[], query: string) {
    return `
insert into ${this.resolveTarget(target)}
(${columns.join(",")})
select ${columns.join(",")}
from (${query}) as insertions`;
  }
}
