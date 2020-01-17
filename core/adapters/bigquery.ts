import { IAdapter } from "@dataform/core/adapters";
import { Adapter } from "@dataform/core/adapters/base";
import { Task, Tasks } from "@dataform/core/tasks";
import { dataform } from "@dataform/protos";

export class BigQueryAdapter extends Adapter implements IAdapter {
  constructor(private project: dataform.IProjectConfig, private dataformCoreVersion: string) {
    super();
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

    this.addPreOps(table, this.dataformCoreVersion, tasks);

    if (table.type === "incremental") {
      if (tableMetadata && tableMetadata.type !== this.baseTableType(table.type)) {
        tasks.add(
          Task.statement(this.dropIfExists(table.target, this.oppositeTableType(table.type)))
        );
      }

      if (runConfig.fullRefresh || !tableMetadata || tableMetadata.type === "view") {
        tasks.add(Task.statement(this.createOrReplace(table)));
      } else {
        tasks.add(
          Task.statement(
            this.insertInto(
              table.target,
              tableMetadata.fields.map(f => f.name),
              this.dataformCoreVersion > "1.4.8"
                ? table.incrementalQuery
                : this.where(table.incrementalQuery || table.query, table.where)
            )
          )
        );
      }
    } else {
      if (tableMetadata && tableMetadata.type !== this.baseTableType(table.type)) {
        tasks.add(
          Task.statement(this.dropIfExists(table.target, this.oppositeTableType(table.type)))
        );
      }
      tasks.add(Task.statement(this.createOrReplace(table)));

      this.addPostOps(table, this.dataformCoreVersion, tasks);
    }

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
