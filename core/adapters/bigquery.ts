import { IAdapter } from "@dataform/core/adapters";
import { Adapter } from "@dataform/core/adapters/base";
import { Task, Tasks } from "@dataform/core/tasks";
import { dataform } from "@dataform/protos";

export class BigQueryAdapter extends Adapter implements IAdapter {
  constructor(private readonly project: dataform.IProjectConfig, dataformCoreVersion: string) {
    super(dataformCoreVersion);
  }

  public resolveTarget(target: dataform.ITarget) {
    return `\`${target.database || this.project.defaultDatabase}.${target.schema ||
      this.project.defaultSchema}.${target.name}\``;
  }

  public publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks {
    const tasks = Tasks.create();

    this.preOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    if (tableMetadata && tableMetadata.type !== this.baseTableType(table.type)) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(table.type)))
      );
    }

    if (table.type === "incremental") {
      if (!this.shouldWriteIncrementally(runConfig, tableMetadata)) {
        tasks.add(Task.statement(this.createOrReplace(table)));
      } else {
        tasks.add(
          Task.statement(
            this.insertInto(
              table.target,
              tableMetadata.fields.map(f => f.name),
              this.where(table.incrementalQuery || table.query, table.where)
            )
          )
        );
      }
    } else {
      tasks.add(Task.statement(this.createOrReplace(table)));
    }

    this.postOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    return tasks;
  }

  public assertTasks(
    assertion: dataform.IAssertion,
    projectConfig: dataform.IProjectConfig
  ): Tasks {
    const tasks = Tasks.create();
    const target =
      assertion.target ||
      dataform.Target.create({
        schema: projectConfig.assertionSchema,
        name: assertion.name
      });
    tasks.add(Task.statement(this.createOrReplaceView(target, assertion.query)));
    tasks.add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
    return tasks;
  }

  public createOrReplace(table: dataform.ITable) {
    return `create or replace ${this.baseTableType(table.type)} ${this.resolveTarget(
      table.target
    )} ${
      table.bigquery && table.bigquery.partitionBy
        ? `partition by ${table.bigquery.partitionBy} `
        : ""
    }${
      table.bigquery && table.bigquery.clusterBy && table.bigquery.clusterBy.length > 0
        ? `cluster by ${table.bigquery.clusterBy.join(", ")} `
        : ""
    }as ${table.query}`;
  }

  public createOrReplaceView(target: dataform.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)}`;
  }
}
