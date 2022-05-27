import { IAdapter } from "df/core/adapters";
import { Adapter } from "df/core/adapters/base";
import { Task, Tasks } from "df/core/tasks";
import { dataform } from "df/protos/ts";

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
    tableMetadata?: dataform.ITableMetadata
  ): Tasks {
    const tasks = Tasks.create();

    this.preOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    const baseTableType = this.baseTableType(table.type);
    if (tableMetadata && tableMetadata.type !== baseTableType) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(baseTableType)))
      );
    }

    if (table.type === "incremental") {
      if (!this.shouldWriteIncrementally(runConfig, tableMetadata)) {
        tasks.add(Task.statement(this.createOrReplace(table)));
      } else if (table.uniqueKey && table.uniqueKey.length > 0 && (table.strategy===null || table.strategy==="merge")){
        tasks.add(
          Task.statement(
            this.mergeInto(
                  table.target,
                  tableMetadata?.fields.map(f => f.name),
                  this.where(table.incrementalQuery || table.query, table.where),
                  table.uniqueKey,
                  table.bigquery && table.bigquery.updatePartitionFilter
                )
          )
        );
      } else {
        if (table.strategy==="insert_overwrite"){
          if(table.overwriteFilter){
            tasks.add(Task.statement(this.deleteWithStaticFilter(table.target,table.overwriteFilter)));
          }else {
            tasks.add(Task.statement(this.deleteDynamically(table.target,table.bigquery.partitionBy,
              this.where(table.incrementalQuery || table.query, table.where))));
          }
        }
        tasks.add(
            Task.statement(
              this.insertInto(
                table.target,
                tableMetadata?.fields.map(f => f.name).map(column => `\`${column}\``),
                this.where(table.incrementalQuery || table.query, table.where)
              )
          )
        );
      }
    } else {
      tasks.add(Task.statement(this.createOrReplace(table)));
    }

    this.postOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    return tasks.concatenate();
  }

  public assertTasks(
    assertion: dataform.IAssertion,
    projectConfig: dataform.IProjectConfig
  ): Tasks {
    const tasks = Tasks.create();
    const target = assertion.target;
    tasks.add(Task.statement(this.createOrReplaceView(target, assertion.query)));
    tasks.add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
    return tasks;
  }

  public dropIfExists(target: dataform.ITarget, type: dataform.TableMetadata.Type) {
    return `drop ${this.tableTypeAsSql(type)} if exists ${this.resolveTarget(target)}`;
  }

  private createOrReplace(table: dataform.ITable) {
    const options = []
    if (table.bigquery && table.bigquery.partitionBy && table.bigquery.partitionExpirationDays){
      options.push(`partition_expiration_days=${table.bigquery.partitionExpirationDays}`)
    }
    if (table.bigquery && table.bigquery.partitionBy && table.bigquery.requirePartitionFilter){
      options.push(`require_partition_filter=${table.bigquery.requirePartitionFilter}`)
    }
    if(table.bigquery && table.bigquery.additionalOptions){
      for(const [optionName, optionValue] of Object.entries(table.bigquery.additionalOptions)){
        options.push(`${optionName}=${optionValue}`)
      }
    }

    return `create or replace ${
      table.materialized
      ? "materialized "
      : ""
    }${this.tableTypeAsSql(
      this.baseTableType(table.type)
    )} ${this.resolveTarget(table.target)} ${
      table.bigquery && table.bigquery.partitionBy
        ? `partition by ${table.bigquery.partitionBy} `
        : ""
    }${
      table.bigquery && table.bigquery.clusterBy && table.bigquery.clusterBy.length > 0
        ? `cluster by ${table.bigquery.clusterBy.join(", ")} `
        : ""
    }${
      options.length>0 ?
      `OPTIONS(${options.join(',')})` : ""
    }as ${table.query}`;
  }

  private createOrReplaceView(target: dataform.ITarget, query: string) {
    return `
      create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  private deleteWithStaticFilter(
    target: dataform.ITarget,
    overwriteFilter: string
  ) {
    return `delete from ${this.resolveTarget(target)} T where ${overwriteFilter}`;
  }

  private deleteDynamically(
    target: dataform.ITarget,
    partitionBy: string,
    query: string
  ) {
    return `delete from ${this.resolveTarget(target)} T where ${partitionBy} in (select ${partitionBy} from (${query}))`;
  }


  private mergeInto(
    target: dataform.ITarget,
    columns: string[],
    query: string,
    uniqueKey: string[],
    updatePartitionFilter: string
  ) {
    const backtickedColumns = columns.map(column => `\`${column}\``);
    return `
merge ${this.resolveTarget(target)} T
using (${query}
) S
on ${uniqueKey.map(uniqueKeyCol => `T.${uniqueKeyCol} = S.${uniqueKeyCol}`).join(` and `)}
  ${updatePartitionFilter ? `and T.${updatePartitionFilter}` : ""}
when matched then
  update set ${columns.map(column => `\`${column}\` = S.${column}`).join(",")}
when not matched then
  insert (${backtickedColumns.join(",")}) values (${backtickedColumns.join(",")})`;
  }
}
