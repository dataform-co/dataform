import { IAdapter } from "df/core/adapters";
import { Adapter } from "df/core/adapters/base";
import { Task, Tasks } from "df/core/tasks";
import { dataform } from "df/protos/ts";

export class SnowflakeAdapter extends Adapter implements IAdapter {
  constructor(private readonly project: dataform.IProjectConfig, dataformCoreVersion: string) {
    super(dataformCoreVersion);
  }

  public resolveTarget(target: dataform.ITarget) {
    return `${!!target.database ? `"${target.database}".` : ""}"${target.schema}"."${target.name}"`;
  }

  public normalizeIdentifier(identifier: string) {
    return identifier.toUpperCase();
  }

  public publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
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
      } else {
        tasks.add(
          Task.statement(
            table.uniqueKey && table.uniqueKey.length > 0
              ? this.mergeInto(
                  table.target,
                  tableMetadata.fields.map(f => f.name),
                  this.where(table.incrementalQuery || table.query, table.where),
                  table.uniqueKey
                )
              : this.insertInto(
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
    tasks.add(Task.statement(this.createOrReplaceView(target, assertion.query, false)));
    tasks.add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
    return tasks;
  }

  private createOrReplaceView(target: dataform.ITarget, query: string, secure: boolean) {
    return `create or replace ${secure ? "secure " : ""}view ${this.resolveTarget(
      target
    )} as ${query}`;
  }

  private createOrReplace(table: dataform.ITable) {
    if (table.type === "view") {
      return this.createOrReplaceView(table.target, table.query, table.snowflake?.secure);
    }
    return `create or replace ${
      table.snowflake?.transient ? "transient " : ""
    }table ${this.resolveTarget(table.target)} ${
      table.snowflake?.clusterBy?.length > 0
        ? `cluster by (${table.snowflake?.clusterBy.join(", ")}) `
        : ""
    }as ${table.query}`;
  }

  private mergeInto(
    target: dataform.ITarget,
    columns: string[],
    query: string,
    uniqueKey: string[]
  ) {
    return `
merge into ${this.resolveTarget(target)} T
using (${query}
) S
on ${uniqueKey.map(uniqueKeyCol => `T.${uniqueKeyCol} = S.${uniqueKeyCol}`).join(` and `)}
when matched then
  update set ${columns.map(column => `${column} = S.${column}`).join(",")}
when not matched then
  insert (${columns.join(",")}) values (${columns.join(",")})`;
  }
}
