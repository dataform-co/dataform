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
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks {
    const tasks = Tasks.create();
    // Drop views/tables first if they exist.
    if (tableMetadata && tableMetadata.type != this.baseTableType(table.type)) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(table.type)))
      );
    }
    if (table.type == "incremental") {
      if (runConfig.fullRefresh || !tableMetadata || tableMetadata.type == "view") {
        tasks.add(Task.statement(this.createOrReplace(table)));
      } else {
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
      tasks.add(Task.statement(this.createOrReplace(table)));
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

  public createEmptyIfNotExists(table: dataform.ITable) {
    return `create ${this.baseTableType(table.type)} if not exists ${this.resolveTarget(
      table.target
    )} ${
      table.bigquery && table.bigquery.partitionBy
        ? `partition by ${table.bigquery.partitionBy}`
        : ""
    } as ${this.where(table.query, "false")}`;
  }

  public createOrReplace(table: dataform.ITable) {
    return `create or replace ${this.baseTableType(table.type)} ${this.resolveTarget(
      table.target
    )} ${
      table.bigquery && table.bigquery.partitionBy
        ? `partition by ${table.bigquery.partitionBy}`
        : ""
    } as ${table.query}`;
  }

  public createOrReplaceView(target: dataform.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)}`;
  }
}
