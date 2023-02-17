import * as semver from "semver";

import { IAdapter } from "df/core/adapters";
import { Adapter } from "df/core/adapters/base";
import { Task, Tasks } from "df/core/tasks";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export class RedshiftAdapter extends Adapter implements IAdapter {
  constructor(private readonly project: core.ProjectConfig, dataformCoreVersion: string) {
    super(dataformCoreVersion);
  }

  public resolveTarget(target: core.Target) {
    return `"${target.schema}"."${target.name}"`;
  }

  public publishTasks(
    table: core.Table,
    runConfig: execution.RunConfig,
    tableMetadata: execution.TableMetadata
  ): Tasks {
    const tasks = Tasks.create();

    this.preOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    const baseTableType = this.baseTableType(table.enumType);
    if (tableMetadata && tableMetadata.type !== baseTableType) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(baseTableType)))
      );
    }

    if (table.enumType === core.TableType.INCREMENTAL) {
      if (!this.shouldWriteIncrementally(runConfig, tableMetadata)) {
        tasks.addAll(this.createOrReplace(table));
      } else {
        tasks.addAll(
          table.uniqueKey && table.uniqueKey.length > 0
            ? this.mergeInto(
                table.target,
                this.where(table.incrementalQuery || table.query, table.where),
                table.uniqueKey
              )
            : Tasks.create().add(
                Task.statement(
                  this.insertInto(
                    table.target,
                    tableMetadata.fields.map(f => f.name),
                    this.where(table.incrementalQuery || table.query, table.where)
                  )
                )
              )
        );
      }
    } else {
      tasks.addAll(this.createOrReplace(table));
    }

    this.postOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    return tasks;
  }

  public assertTasks(assertion: core.Assertion, projectConfig: core.ProjectConfig): Tasks {
    const target = assertion.target;
    return Tasks.create()
      .add(Task.statement(this.dropIfExists(target, execution.TableMetadata_Type.VIEW)))
      .add(Task.statement(this.createOrReplaceView(target, assertion.query, false)))
      .add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
  }

  public dropIfExists(target: core.Target, type: execution.TableMetadata_Type) {
    const query = `drop ${this.tableTypeAsSql(type)} if exists ${this.resolveTarget(target)}`;
    if (this.project.warehouse === "postgres" || this.isBindSupported()) {
      return `${query} cascade`;
    }
    return query;
  }

  private createOrReplaceView(target: core.Target, query: string, bind: boolean) {
    const createQuery = `create or replace view ${this.resolveTarget(target)} as ${query}`;
    // Postgres doesn't support with no schema binding.
    if (bind || this.project.warehouse === "postgres") {
      return createQuery;
    }
    return `${createQuery} with no schema binding`;
  }

  private createOrReplace(table: core.Table) {
    if (table.enumType === core.TableType.VIEW) {
      const isBindDefined = table.redshift && table.redshift.hasOwnProperty("bind");
      const bindDefaultValue = semver.gte(this.dataformCoreVersion, "1.4.1") ? false : true;
      const bind =
        (isBindDefined ? table.redshift.bind : bindDefaultValue) && this.isBindSupported();
      return (
        Tasks.create()
          // Drop the view in case we are changing the number of column(s) (or their types).
          .add(Task.statement(this.dropIfExists(table.target, this.baseTableType(table.enumType))))
          .add(Task.statement(this.createOrReplaceView(table.target, table.query, bind)))
      );
    }
    const tempTableTarget = core.Target.create({
      schema: table.target.schema,
      name: table.target.name + "_temp"
    });

    return Tasks.create()
      .add(Task.statement(this.dropIfExists(tempTableTarget, this.baseTableType(table.enumType))))
      .add(Task.statement(this.createTable(table, tempTableTarget)))
      .add(Task.statement(this.dropIfExists(table.target, execution.TableMetadata_Type.TABLE)))
      .add(
        Task.statement(
          `alter table ${this.resolveTarget(tempTableTarget)} rename to "${table.target.name}"`
        )
      );
  }

  private createTable(table: core.Table, target: core.Target) {
    if (table.redshift) {
      let query = `create table ${this.resolveTarget(target)}`;

      if (table.redshift.distStyle && table.redshift.distKey) {
        query = `${query} diststyle ${table.redshift.distStyle} distkey (${table.redshift.distKey})`;
      }
      if (table.redshift.sortStyle && table.redshift.sortKeys) {
        query = `${query} ${table.redshift.sortStyle} sortkey (${table.redshift.sortKeys.join(
          ", "
        )})`;
      }

      return `${query} as ${table.query}`;
    }

    return `create table ${this.resolveTarget(target)} as ${table.query}`;
  }

  private mergeInto(target: core.Target, query: string, uniqueKey: string[]) {
    const finalTarget = this.resolveTarget(target);
    // Schema name not allowed for temporary tables.
    const tempTarget = `"${target.schema}__${target.name}_incremental_temp"`;
    return Tasks.create()
      .add(Task.statement(`drop table if exists ${tempTarget};`))
      .add(
        Task.statement(`create temp table ${tempTarget} as select * from (${query}
) as data;`)
      )
      .add(Task.statement(`begin transaction;`))
      .add(
        Task.statement(
          `delete from ${finalTarget} using ${tempTarget} where ${uniqueKey
            .map(
              uniqueKeyCol => `${finalTarget}."${uniqueKeyCol}" = ${tempTarget}."${uniqueKeyCol}"`
            )
            .join(` and `)};`
        )
      )
      .add(Task.statement(`insert into ${finalTarget} select * from ${tempTarget};`))
      .add(Task.statement(`end transaction;`))
      .add(Task.statement(`drop table ${tempTarget};`));
  }

  private isBindSupported() {
    return semver.lte(this.dataformCoreVersion, "1.10.0");
  }
}
