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
    return `"${target.schema || this.project.defaultSchema}"."${target.name}"`;
  }

  public publishTasks(
    t: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks {
    const tasks = Tasks.create();
    // Drop the existing view or table if we are changing it's type.
    if (tableMetadata && tableMetadata.type != this.baseTableType(t.type)) {
      tasks.add(Task.statement(this.dropIfExists(t.target, this.oppositeTableType(t.type))));
    }
    if (t.type == "incremental") {
      if (runConfig.fullRefresh || !tableMetadata || tableMetadata.type == "view") {
        tasks.addAll(this.createOrReplace(t));
      } else {
        // The table exists, insert new rows.
        tasks.add(
          Task.statement(
            this.insertInto(
              t.target,
              tableMetadata.fields.map(f => f.name),
              this.where(t.query, t.where)
            )
          )
        );
      }
    } else {
      tasks.addAll(this.createOrReplace(t));
    }
    return tasks;
  }

  public assertTasks(a: dataform.IAssertion, projectConfig: dataform.IProjectConfig): Tasks {
    const tasks = Tasks.create();
    const assertionTarget = dataform.Target.create({
      schema: projectConfig.assertionSchema,
      name: a.name
    });
    tasks.add(Task.statement(this.createOrReplaceView(assertionTarget, a.query)));
    tasks.add(
      Task.assertion(`select count(*) as row_count from ${this.resolveTarget(assertionTarget)}`)
    );
    return tasks;
  }

  public createOrReplaceView(target: dataform.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  public createOrReplace(t: dataform.ITable) {
    if (t.type == "view") {
      return Tasks.create().add(
        Task.statement(`
        create or replace view ${this.resolveTarget(t.target)}
        as ${t.query}`)
      );
    } else {
      const tempTableTarget = dataform.Target.create({
        schema: t.target.schema,
        name: t.target.name + "_temp"
      });

      const tasks = Tasks.create();
      tasks.add(Task.statement(this.dropIfExists(tempTableTarget, this.baseTableType(t.type))));
      tasks.add(Task.statement(this.createTable(t, tempTableTarget)));
      tasks.add(Task.statement(this.dropIfExists(t.target, "table")));
      tasks.add(
        Task.statement(
          `alter table ${this.resolveTarget(tempTableTarget)} rename to "${t.target.name}"`
        )
      );
      return tasks;
    }
  }

  public createTable(t: dataform.ITable, target: dataform.ITarget) {
    if (t.redshift) {
      let query = `create table ${this.resolveTarget(target)}`;

      if (t.redshift.distStyle && t.redshift.distKey) {
        query = `${query} diststyle ${t.redshift.distStyle} distkey (${t.redshift.distKey})`;
      }
      if (t.redshift.sortStyle && t.redshift.sortKeys) {
        query = `${query} ${t.redshift.sortStyle} sortkey (${t.redshift.sortKeys.join(", ")})`;
      }

      return `${query} as ${t.query}`;
    }

    return `create table ${this.resolveTarget(target)} as ${t.query}`;
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)} cascade`;
  }
}
