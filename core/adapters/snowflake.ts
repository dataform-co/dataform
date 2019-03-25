import * as protos from "@dataform/protos";
import { Task, Tasks } from "../tasks";
import { Adapter } from "./base";
import { IAdapter } from "./index";

export class SnowflakeAdapter extends Adapter implements IAdapter {
  private project: protos.IProjectConfig;

  constructor(project: protos.IProjectConfig) {
    super();
    this.project = project;
  }

  public resolveTarget(target: protos.ITarget) {
    return `"${target.schema || this.project.defaultSchema}"."${target.name}"`;
  }

  public publishTasks(
    t: protos.ITable,
    runConfig: protos.IRunConfig,
    tableMetadata: protos.ITableMetadata
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

  public assertTasks(a: protos.IAssertion, projectConfig: protos.IProjectConfig): Tasks {
    const tasks = Tasks.create();
    const assertionTarget = protos.Target.create({
      schema: projectConfig.assertionSchema,
      name: a.name
    });
    tasks.add(Task.statement(this.createOrReplaceView(assertionTarget, a.query)));
    tasks.add(
      Task.assertion(`select count(*) as row_count from ${this.resolveTarget(assertionTarget)}`)
    );
    return tasks;
  }

  public createOrReplaceView(target: protos.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  public createOrReplace(t: protos.ITable) {
    return `create or replace ${this.baseTableType(t.type || "table")} ${this.resolveTarget(
      t.target
    )} as ${t.query}`;
  }
}
