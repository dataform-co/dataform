import { dataform } from "@dataform/protos";
import { Task, Tasks } from "../tasks";
import { Adapter } from "./base";
import { IAdapter } from "./index";

export class SnowflakeAdapter extends Adapter implements IAdapter {
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
        tasks.add(Task.statement(this.createOrReplace(t)));
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
      tasks.add(Task.statement(this.createOrReplace(t)));
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
    return `create or replace ${this.baseTableType(t.type || "table")} ${this.resolveTarget(
      t.target
    )} as ${t.query}`;
  }
}
