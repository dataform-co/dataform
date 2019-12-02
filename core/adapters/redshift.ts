import { IAdapter } from "@dataform/core/adapters";
import { Adapter } from "@dataform/core/adapters/base";
import { Task, Tasks } from "@dataform/core/tasks";
import { dataform } from "@dataform/protos";
import * as semver from "semver";

export class RedshiftAdapter extends Adapter implements IAdapter {
  constructor(private project: dataform.IProjectConfig, private dataformCoreVersion: string) {
    super();
  }

  public resolveTarget(target: dataform.ITarget) {
    return `"${target.schema}"."${target.name}"`;
  }

  public publishTasks(
    table: dataform.ITable,
    runConfig: dataform.IRunConfig,
    tableMetadata: dataform.ITableMetadata
  ): Tasks {
    const tasks = Tasks.create();
    // Drop the existing view or table if we are changing it's type.
    if (tableMetadata && tableMetadata.type !== this.baseTableType(table.type)) {
      tasks.add(
        Task.statement(this.dropIfExists(table.target, this.oppositeTableType(table.type)))
      );
    }
    if (table.type === "incremental") {
      if (runConfig.fullRefresh || !tableMetadata || tableMetadata.type === "view") {
        tasks.addAll(this.createOrReplace(table));
      } else {
        // The table exists, insert new rows.
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
      tasks.addAll(this.createOrReplace(table));
    }
    return tasks;
  }

  public assertTasks(
    assertion: dataform.IAssertion,
    projectConfig: dataform.IProjectConfig
  ): Tasks {
    const target =
      assertion.target ||
      dataform.Target.create({
        schema: projectConfig.assertionSchema,
        name: assertion.name
      });
    return Tasks.create()
      .add(Task.statement(this.createOrReplaceView(target, assertion.query, true)))
      .add(Task.assertion(`select sum(1) as row_count from ${this.resolveTarget(target)}`));
  }

  public createOrReplaceView(target: dataform.ITarget, query: string, bind: boolean) {
    const createQuery = `create or replace view ${this.resolveTarget(target)} as ${query}`;
    if (bind) {
      return createQuery;
    }
    return `${createQuery} with no schema binding`;
  }

  public createOrReplace(table: dataform.ITable) {
    if (table.type === "view") {
      const isBindDefined = table.redshift && table.redshift.hasOwnProperty("bind");
      const bindDefaultValue = semver.gte(this.dataformCoreVersion, "1.4.1") ? false : true;
      const bind = isBindDefined ? table.redshift.bind : bindDefaultValue;
      return (
        Tasks.create()
          // Drop the view in case we are changing the number of column(s) (or their types).
          .add(Task.statement(this.dropIfExists(table.target, this.baseTableType(table.type))))
          .add(Task.statement(this.createOrReplaceView(table.target, table.query, bind)))
      );
    }
    const tempTableTarget = dataform.Target.create({
      schema: table.target.schema,
      name: table.target.name + "_temp"
    });

    return Tasks.create()
      .add(Task.statement(this.dropIfExists(tempTableTarget, this.baseTableType(table.type))))
      .add(Task.statement(this.createTable(table, tempTableTarget)))
      .add(Task.statement(this.dropIfExists(table.target, "table")))
      .add(
        Task.statement(
          `alter table ${this.resolveTarget(tempTableTarget)} rename to "${table.target.name}"`
        )
      );
  }

  public createTable(table: dataform.ITable, target: dataform.ITarget) {
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

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)} cascade`;
  }
}
