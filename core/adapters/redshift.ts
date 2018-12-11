import * as protos from "@dataform/protos";
import { Adapter } from "./index";
import { Task, Tasks } from "../tasks";

export class RedshiftAdapter implements Adapter {
  private project: protos.IProjectConfig;

  constructor(project: protos.IProjectConfig) {
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
        tasks.addAll(this.createOrReplace(m));
      } else {
        // The table exists, insert new rows.
        tasks.add(Task.statement(this.insertInto(m.target, Object.keys(m.descriptor), this.where(m.query, m.where))));
      }
    } else {
      tasks.addAll(this.createOrReplace(m));
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
    if (m.type == "view") {
      return Tasks.create().add(
        Task.statement(`
        create or replace view ${this.resolveTarget(m.target)}
        as ${m.query}`)
      );
    } else {
      const tempTableTarget = protos.Target.create({
        schema: m.target.schema,
        name: m.target.name + "_temp"
      });

      const tasks = Tasks.create();
      tasks.add(Task.statement(this.dropIfExists(tempTableTarget, this.baseTableType(m.type))));
      tasks.add(Task.statement(this.createTable(m, tempTableTarget)));
      tasks.add(Task.statement(this.dropIfExists(m.target, "table")));
      tasks.add(Task.statement(`alter table ${this.resolveTarget(tempTableTarget)} rename to "${m.target.name}"`));
      return tasks;
    }
  }

  createTable(m: protos.IMaterialization, tempTableTarget) {
    if (m.redshift) {
      const prefix = `create table ${this.resolveTarget(tempTableTarget)}`;
      const distPart = `diststyle ${m.redshift.distStyle} distkey (${m.redshift.distKey})`;
      const sortPart = `${m.redshift.sortStyle} sortkey (${m.redshift.sortKeys.join(", ")})`;

      return `${prefix} ${distPart} ${sortPart} as ${m.query}`;
    }

    return `create table ${this.resolveTarget(tempTableTarget)} as ${m.query}`;
  }

  insertInto(target: protos.ITarget, columns: string[], query: string) {
    return `
      insert into ${this.resolveTarget(target)}
      (${columns.join(",")})
      ${query}`;
  }

  dropIfExists(target: protos.ITarget, type: string) {
    return `drop ${type} if exists ${this.resolveTarget(target)}`;
  }

  where(query: string, where: string) {
    return `select * from (
        ${query})
        where ${where}`;
  }

  baseTableType(type: string) {
    if (type == "incremental") {
      return "table";
    }
    return type;
  }

  oppositeTableType(type: string) {
    return this.baseTableType(type) == "table" ? "view" : "table";
  }
}
