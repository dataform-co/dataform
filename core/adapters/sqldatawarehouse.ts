import { Adapter } from "@dataform/core/adapters/base";
import { IAdapter } from "@dataform/core/adapters/index";
import { Task, Tasks } from "@dataform/core/tasks";
import { dataform } from "@dataform/protos";

export class SQLDataWarehouseAdapter extends Adapter implements IAdapter {
  constructor(private project: dataform.IProjectConfig, private dataformCoreVersion: string) {
    super();
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

    (table.incrementalPreOps || []).forEach(pre => tasks.add(Task.statement(pre)));
    (table.preOps || []).forEach(pre => tasks.add(Task.statement(pre)));

    // Drop the existing view or table if we are changing its type.
    if (tableMetadata && tableMetadata.type !== this.baseTableType(table.type)) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(table.type)))
      );
    }
    if (table.type === "incremental") {
      if (runConfig.fullRefresh || !tableMetadata || tableMetadata.type === "view") {
        tasks.addAll(this.createOrReplace(table, !!tableMetadata));
      } else {
        // The table exists, insert new rows.
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

    (table.incrementalPostOps || []).forEach(post => tasks.add(Task.statement(post)));
    (table.postOps || []).forEach(post => tasks.add(Task.statement(post)));

    return tasks;
  }

  public assertTasks(
    assertion: dataform.IAssertion,
    projectConfig: dataform.IProjectConfig
  ): Tasks {
    const target =
      assertion.target ||
      dataform.Target.create({
        schema: projectConfig.assertionSchema,
        name: assertion.name
      });

    return Tasks.create()
      .add(Task.statement(this.dropIfExists(target, "view")))
      .add(
        Task.statement(`
        create view ${this.resolveTarget(target)}
        as ${assertion.query}`)
      )
      .add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    if (type === "view") {
      return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)} `;
    }
    return `if object_id ('${this.resolveTarget(
      target
    )}','U') is not null drop table ${this.resolveTarget(target)}`;
  }

  private createOrReplace(table: dataform.ITable, alreadyExists: boolean) {
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
      .add(Task.statement(this.dropIfExists(table.target, "table")))
      .add(
        Task.statement(
          `rename object ${this.resolveTarget(tempTableTarget)} to ${table.target.name} `
        )
      );
  }

  private createTable(table: dataform.ITable, target: dataform.ITarget) {
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
