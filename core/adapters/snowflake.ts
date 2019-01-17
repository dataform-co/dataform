import * as protos from "@dataform/protos";
import { IAdapter } from "./index";
import { Adapter } from "./base";
import { Task, Tasks } from "../tasks";

export class SnowflakeAdapter extends Adapter implements IAdapter {
  private project: protos.IProjectConfig;

  constructor(project: protos.IProjectConfig) {
    super();
    this.project = project;
  }

  resolveTarget(target: protos.ITarget) {
    return `"${target.schema || this.project.defaultSchema}"."${target.name}"`;
  }

  materializeTasks(m: protos.IMaterialization, runConfig: protos.IRunConfig, table: protos.ITable): Tasks {
    var tasks = Tasks.create();
    // Drop the existing view or table if we are changing it's type.
    if (table && table.type != this.baseTableType(m.type)) {
      tasks.add(Task.statement(this.dropIfExists(m.target, this.oppositeTableType(m.type))));
    }
    if (m.type == "incremental") {
      if (runConfig.fullRefresh || !table || table.type == "view") {
        tasks.add(Task.statement(this.createOrReplace(m)));
      } else {
        // The table exists, insert new rows.
        tasks.add(Task.statement(this.insertInto(m.target, Object.keys(m.descriptor), this.where(m.query, m.where))));
      }
    } else {
      tasks.add(Task.statement(this.createOrReplace(m)));
    }
    return tasks;
  }

  assertTasks(a: protos.IAssertion, projectConfig: protos.IProjectConfig): Tasks {
    var tasks = Tasks.create();
    var assertionTarget = protos.Target.create({
      schema: projectConfig.assertionSchema,
      name: a.name
    });
    tasks.add(Task.statement(this.createOrReplaceView(assertionTarget, a.query)));
    tasks.add(Task.assertion(`select 1 as detect from ${this.resolveTarget(assertionTarget)}`));
    return tasks;
  }

  createOrReplaceView(target: protos.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  createOrReplace(m: protos.IMaterialization) {
    return `create or replace ${this.baseTableType(m.type || "table")} ${this.resolveTarget(m.target)} as ${m.query}`;
  }

  insertInto(target: protos.ITarget, columns: string[], query: string) {
    return `
      insert into ${this.resolveTarget(target)}
      (${columns.join(",")})
      ${query}`;
  }

  dropIfExists(target: protos.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)}`;
  }
}
