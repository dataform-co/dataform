import { dataform } from "@dataform/protos";
import { Task, Tasks } from "../tasks";
import { Adapter } from "./base";
import { IAdapter } from "./index";

export class RedshiftAdapter extends Adapter implements IAdapter {
  private project: dataform.IProjectConfig;

  constructor(project: dataform.IProjectConfig) {
    super();
    this.project = project;
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
    // Drop the existing view or table if we are changing it's type.
    if (tableMetadata && tableMetadata.type != this.baseTableType(table.type)) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(table.type)))
      );
    }
    if (table.type == "incremental") {
      if (runConfig.fullRefresh || !tableMetadata || tableMetadata.type == "view") {
        tasks.addAll(this.createOrReplace(table));
      } else {
        // The table exists, insert new rows.
        tasks.add(
          Task.statement(
            this.insertInto(
              table.target,
              tableMetadata.fields.map(f => f.name),
              this.where(table.query, table.where)
            )
          )
        );
      }
    } else {
      tasks.addAll(this.createOrReplace(table));
    }
    return tasks;
  }

  public assertTasks(
    assertion: dataform.IAssertion,
    projectConfig: dataform.IProjectConfig
  ): Tasks {
    const tasks = Tasks.create();
    tasks.add(Task.statement(this.createOrReplaceView(assertion.target, assertion.query)));
    tasks.add(
      Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(assertion.target)}`)
    );
    return tasks;
  }

  public createOrReplaceView(target: dataform.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  public createOrReplace(table: dataform.ITable) {
    if (table.type == "view") {
      return Tasks.create().add(
        Task.statement(`
        create or replace view ${this.resolveTarget(table.target)}
        as ${table.query}`)
      );
    } else {
      const tempTableTarget = dataform.Target.create({
        schema: table.target.schema,
        name: table.target.name + "_temp"
      });

      const tasks = Tasks.create();
      tasks.add(Task.statement(this.dropIfExists(tempTableTarget, this.baseTableType(table.type))));
      tasks.add(Task.statement(this.createTable(table, tempTableTarget)));
      tasks.add(Task.statement(this.dropIfExists(table.target, "table")));
      tasks.add(
        Task.statement(
          `alter table ${this.resolveTarget(tempTableTarget)} rename to "${table.target.name}"`
        )
      );
      return tasks;
    }
  }

  public createTable(table: dataform.ITable, target: dataform.ITarget) {
    if (table.redshift) {
      let query = `create table ${this.resolveTarget(target)}`;

      if (table.redshift.distStyle && table.redshift.distKey) {
        query = `${query} diststyle ${table.redshift.distStyle} distkey (${
          table.redshift.distKey
        })`;
      }
      if (table.redshift.sortStyle && table.redshift.sortKeys) {
        query = `${query} ${table.redshift.sortStyle} sortkey (${table.redshift.sortKeys.join(
          ", "
        )})`;
      }

      return `${query} as ${table.query}`;
    }

    return `create table ${this.resolveTarget(target)} as ${table.query}`;
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)} cascade`;
  }
}
