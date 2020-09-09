import { IAdapter } from "df/core/adapters";
import { Adapter } from "df/core/adapters/base";
import { Task, Tasks } from "df/core/tasks";
import { dataform } from "df/protos/ts";

export class PrestoAdapter extends Adapter implements IAdapter {
  constructor(private readonly project: dataform.IProjectConfig, dataformCoreVersion: string) {
    super(dataformCoreVersion);
  }

  // Connectors are mapped to catalogs, sources of data, for example Postgres, Hive or BigQuery.
  // A connection can have multiple catalogs, allowing data to be accessed from multiple sources,
  // even in the same query.

  // Catalogs are collections of schemas.

  // Catalog.schema.table_name

  // Statements are the textual representation of an SQL query; Presto then delivers it as queries how it sees fit.

  // Query > Stage > Task > (split, driver), (operator, exchange)

  // Unlike with the other adapters, the catalog and schema are specified during connection.
  public resolveTarget(target: dataform.ITarget) {
    // Database here is equivalent to Presto's catalog.
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
      throw new Error("Incremental table types are not currently supported for Presto.");
    } else {
      tasks.add(Task.statement(this.createOrReplace(table)));
    }

    this.postOps(table, runConfig, tableMetadata).forEach(statement => tasks.add(statement));

    if (runConfig.useSingleQueryPerAction) {
      return tasks.concatenate();
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

  public createOrReplaceView(target: dataform.ITarget, query: string) {
    return `create or replace view ${this.resolveTarget(target)} as ${query}`;
  }

  public createTable(table: dataform.ITable) {
    return `create table if not exists ${this.resolveTarget(table.target)} as ${table.query} ${
      table.presto?.partitionBy
        ? `with (partitioned_by = array[${table.presto?.partitionBy.join(", ")}]`
        : ""
    })`;
  }

  public dropTableIfExists(target: dataform.ITarget, type: dataform.TableMetadata.Type) {
    return `drop ${this.tableTypeAsSql(type)} if exists ${this.resolveTarget(target)}`;
  }

  public createOrReplace(table: dataform.ITable) {
    // TODO: Not all connectors support replacing/dropping tables, but DROP is supported.
    // A solution should be thought about for handling presto connections to see whether the target
    // catalog allows dropping. For now default to creating the table.
    return this.createTable(table);
  }

  public mergeInto() {
    // TODO: merge isn't supported by Presto. Instead a solution could be formed by using a temporary
    // table (as we do in Redshift) using an in memory connection, as suggested here?
    // https://groups.google.com/g/presto-users/c/5I480n_1nPc
  }
}
