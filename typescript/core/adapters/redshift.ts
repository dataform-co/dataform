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

  buildTasks(m: protos.IMaterialization, runConfig: protos.IRunConfig, table: protos.ITable): Tasks {
    var tasks = Tasks.create();
    // Drop the existing view or table if we are changing it's type.
    if (table.type && table.type != this.baseTableType(m.type)) {
      tasks.add(Task.statement(this.dropIfExists(m.target, this.oppositeTableType(m.type))));
    }
    if (m.type == "incremental") {
      if (runConfig.fullRefresh || !table.type || table.type == "view") {
        tasks.addAll(this.createOrReplace(m, table));
      } else {
        // The table exists, insert new rows.
        tasks.add(Task.statement(this.insertInto(m.target, m.parsedColumns, this.where(m.query, m.where))));
      }
    } else {
      tasks.addAll(this.createOrReplace(m, table));
    }
    return tasks;
  }

  createOrReplace(m: protos.IMaterialization, table: protos.ITable) {
    if (m.type == "view") {
      return Tasks.create().add(
        Task.statement(`
        create or replace view ${this.resolveTarget(m.target)}
        as ${m.query}`)
      );
    } else {
      var tempTableTarget = protos.Target.create({
        schema: m.target.schema,
        name: m.target.name + "_temp"
      });
      var tasks = Tasks.create();
      tasks.add(Task.statement(this.dropIfExists(tempTableTarget, this.baseTableType(m.type))));
      tasks.add(
        Task.statement(`
        create table ${this.resolveTarget(tempTableTarget)}
        as ${m.query}`)
      );
      tasks.add(Task.statement(this.dropIfExists(m.target, "table")));
      tasks.add(Task.statement(`alter table ${this.resolveTarget(tempTableTarget)} rename to "${m.target.name}"`));
      return tasks;
    }
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
