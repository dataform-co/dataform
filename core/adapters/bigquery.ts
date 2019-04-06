import { dataform } from "@dataform/protos";
import { Task, Tasks } from "../tasks";
import { Adapter } from "./base";
import { IAdapter } from "./index";

export class BigQueryAdapter extends Adapter implements IAdapter {
  private project: dataform.IProjectConfig;

  constructor(project: dataform.IProjectConfig) {
    super();
    this.project = project;
  }

  public resolveTarget(target: dataform.ITarget) {
    return `\`${
      this.project.gcloudProjectId ? `${this.project.gcloudProjectId}.` : ""
    }${target.schema || this.project.defaultSchema}.${target.name}\``;
  }

  public publishTasks(
    t: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks {
    const tasks = Tasks.create();
    // Drop views/tables first if they exist.
    if (tableMetadata && tableMetadata.type != this.baseTableType(t.type)) {
      tasks.add(Task.statement(this.dropIfExists(t.target, this.oppositeTableType(t.type))));
    }
    if (t.type == "incremental") {
      if (runConfig.fullRefresh || !tableMetadata || tableMetadata.type == "view") {
        tasks.add(Task.statement(this.createOrReplace(t)));
      } else {
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
      Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(assertionTarget)}`)
    );
    return tasks;
  }

  public createEmptyIfNotExists(t: dataform.ITable) {
    return `create ${this.baseTableType(t.type)} if not exists ${this.resolveTarget(t.target)} ${
      t.bigquery && t.bigquery.partitionBy ? `partition by ${t.bigquery.partitionBy}` : ""
    } as ${this.where(t.query, "false")}`;
  }

  public createOrReplace(t: dataform.ITable) {
    return `create or replace ${this.baseTableType(t.type)} ${this.resolveTarget(t.target)} ${
      t.bigquery && t.bigquery.partitionBy ? `partition by ${t.bigquery.partitionBy}` : ""
    } as ${t.query}`;
  }

  public createOrReplaceView(target: dataform.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)}`;
  }
}
