import * as protos from "@dataform/protos";
import { IAdapter } from "./index";
import { Adapter } from "./base";
import { Task, Tasks } from "../tasks";

export class BigQueryAdapter extends Adapter implements IAdapter {
  private project: protos.IProjectConfig;

  constructor(project: protos.IProjectConfig) {
    super();
    this.project = project;
  }

  resolveTarget(target: protos.ITarget) {
    return `\`${this.project.gcloudProjectId ? `${this.project.gcloudProjectId}.` : ""}${target.schema ||
      this.project.defaultSchema}.${target.name}\``;
  }

  publishTasks(t: protos.ITable, runConfig: protos.IRunConfig, tableMetadata: protos.ITableMetadata): Tasks {
    var tasks = Tasks.create();
    // Drop views/tables first if they exist.
    if (tableMetadata && tableMetadata.type != this.baseTableType(t.type)) {
      tasks.add(Task.statement(this.dropIfExists(t.target, this.oppositeTableType(t.type))));
    }
    if (t.type == "incremental") {
      if (runConfig.fullRefresh || !tableMetadata || tableMetadata.type == "view") {
        tasks.add(Task.statement(this.createOrReplace(t)));
      } else {
        tasks.add(
          Task.statement(this.insertInto(t.target, tableMetadata.fields.map(f => f.name), this.where(t.query, t.where)))
        );
      }
    } else {
      tasks.add(Task.statement(this.createOrReplace(t)));
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
    tasks.add(Task.assertion(`select count(*) as rows_number from ${this.resolveTarget(assertionTarget)}`));
    return tasks;
  }

  createEmptyIfNotExists(t: protos.ITable) {
    return `create ${this.baseTableType(t.type)} if not exists ${this.resolveTarget(t.target)} ${
      t.bigquery && t.bigquery.partitionBy ? `partition by ${t.bigquery.partitionBy}` : ""
    } as ${this.where(t.query, "false")}`;
  }

  createOrReplace(t: protos.ITable) {
    return `create or replace ${this.baseTableType(t.type)} ${this.resolveTarget(t.target)} ${
      t.bigquery && t.bigquery.partitionBy ? `partition by ${t.bigquery.partitionBy}` : ""
    } as ${t.query}`;
  }

  createOrReplaceView(target: protos.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  dropIfExists(target: protos.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)}`;
  }
}
