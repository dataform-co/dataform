import * as protos from "@dataform/protos";
import { Adapter } from "./index";
import { Task, Tasks } from "../tasks";

export class BigQueryAdapter implements Adapter {
  private project: protos.IProjectConfig;

  constructor(project: protos.IProjectConfig) {
    this.project = project;
  }

  resolveTarget(target: protos.ITarget) {
    return `\`${target.schema || this.project.defaultSchema}.${target.name}\``;
  }

  buildTasks(m: protos.IMaterialization, runConfig: protos.IRunConfig, table: protos.ITable): Tasks {
    var tasks = Tasks.create();
    // Drop views/tables first if they exist.
    tasks.add(Task.statement(this.dropIfExists(m.target, this.oppositeTableType(m.type))).ignoreErrors(true));
    if (m.type == "incremental") {
      tasks.addAll(this.materializeIncremental(m, runConfig.fullRefresh));
    } else {
      tasks.add(Task.statement(this.createOrReplace(m)));
    }
    return tasks;
  }

  materializeIncremental(m: protos.IMaterialization, fullRefresh: boolean): Tasks {
    var tasks = Tasks.create();
    if (fullRefresh) {
      tasks.add(Task.statement(this.createOrReplace(m)));
    } else {
      tasks
        .add(Task.statement(this.createEmptyIfNotExists(m)))
        .add(Task.statement(this.insertInto(m.target, m.parsedColumns, this.where(m.query, m.where))));
    }
    return tasks;
  }

  createEmptyIfNotExists(m: protos.IMaterialization) {
    return `
      create ${this.baseTableType(m.type)} if not exists ${this.resolveTarget(m.target)}
      ${m.partitionBy ? `partition by ${m.partitionBy}` : ""}
      as ${this.where(m.query, "false")}`;
  }

  createOrReplace(m: protos.IMaterialization) {
    return `
      create or replace ${this.baseTableType(m.type)} ${this.resolveTarget(m.target)}
      ${m.partitionBy ? `partition by ${m.partitionBy}` : ""}
      as ${m.query}`;
  }

  insertInto(target: protos.ITarget, columns: string[], query: string) {
    return `
      insert ${this.resolveTarget(target)} (${columns.join(",")})
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
