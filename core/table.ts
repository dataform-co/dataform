import {
  IColumnsDescriptor,
  ICommonContext,
  IDependenciesConfig,
  IDocumentableConfig,
  ITargetableConfig,
  Resolvable
} from "@dataform/core/common";
import { Contextable } from "@dataform/core/common";
import { mapToColumnProtoArray, Session } from "@dataform/core/session";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

/**
 * @hidden
 */
export const TableType = ["table", "view", "incremental", "inline"] as const;
/**
 * Supported types of table actions.
 *
 * Tables of type "view" will be created as views.
 *
 * Tables of type "table" will be created as tables.
 *
 * Tables of type "incremental" must have a where clause provided. For more information, see the [incremental tables guide](guides/incremental-datasets).
 */
export type TableType = typeof TableType[number];

/**
 * @hidden
 */
export const DistStyleType = ["even", "key", "all"] as const;
/**
 * Valid types for setting the distribution style for Redshift tables.
 *
 * View the [Redshift documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html#r_CREATE_TABLE_NEW-diststyle-distkey-and-sortkey-options) for more information.
 */
export type DistStyleType = typeof DistStyleType[number];

/**
 * @hidden
 */
export const SortStyleType = ["compound", "interleaved"] as const;
/**
 * Valid types for setting the sort style for Redshift tables.
 *
 * View the [Redshift documentation](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html#r_CREATE_TABLE_NEW-diststyle-distkey-and-sortkey-options) for more information.
 */
export type SortStyleType = typeof SortStyleType[number];

/**
 * Redshift specific warehouse options.
 */
export interface IRedshiftOptions {
  /**
   * Sets the DISTKEY property when creating tables.
   *
   * For more information, read the [Redshift create table docs](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html#r_CREATE_TABLE_NEW-diststyle-distkey-and-sortkey-options).
   */
  distKey?: string;

  /**
   * Set the DISTSTYLE property when creating tables.
   *
   * For more information, read the [Redshift create table docs](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html#r_CREATE_TABLE_NEW-diststyle-distkey-and-sortkey-options).
   */
  distStyle?: string;

  /**
   * A list of string values that will configure the SORTKEY property when creating tables.
   *
   * For more information, read the [Redshift create table docs](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html#r_CREATE_TABLE_NEW-diststyle-distkey-and-sortkey-options).
   */
  sortKeys?: string[];

  /**
   * Sets the style of the sort key when using sort keys.
   *
   * For more information, read the [Redshift sort style article](https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data-compare-sort-styles.html).
   */
  sortStyle?: string;

  /**
   * By default, views are created as late binding views.
   *
   * When this is set to true, views will not be created as late binding views, and the `WITH SCHEMA BINDING` suffix is omitted.
   *
   * For more information, read the [Redshift create view docs](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_VIEW.html).
   */
  bind?: boolean;
}

/**
 * Options for creating tables within Azure SQL Data Warehouse projects.
 */
export interface ISQLDataWarehouseOptions {
  /**
   * The distribution option value.
   *
   * For more information, read the [Azure CTAS docs](https://docs.microsoft.com/en-gb/sql/t-sql/statements/create-table-as-select-azure-sql-data-warehouse?view=aps-pdw-2016-au7#examples-for-table-distribution).
   */
  distribution?: string;
}

/**
 * Options for creating tables within BigQuery projects.
 */
export interface IBigQueryOptions {
  /**
   * The key with which to partition the table. Typically the name of a timestamp or date column.
   *
   * For more information, read the [BigQuery partitioned tables docs](https://cloud.google.com/bigquery/docs/partitioned-tables).
   */
  partitionBy?: string;
}

/**
 * Configuration options for datasets, including tables and views.
 */
export interface ITableConfig extends ITargetableConfig, IDocumentableConfig, IDependenciesConfig {
  /**
   * The type of the dataset. For more information on how this setting works, check out some of the [guides](guides)
   * on publishing different types of datasets with Dataform.
   */
  type?: TableType;

  /**
   * If set to true, this action will not be executed. However, the action may still be depended upon.
   * Useful for temporarily turning off broken actions.
   */
  disabled?: boolean;

  /**
   * Only allowed when the table type is `incremental`.
   *
   * If set to true, running this action will ignore the full-refresh option.
   * This is useful for tables which are built from transient data, to ensure that historical data is never lost.
   */
  protected?: boolean;

  /**
   * Redshift-specific warehouse options.
   */
  redshift?: IRedshiftOptions;

  /**
   * BigQuery-specific warehouse options.
   */
  bigquery?: IBigQueryOptions;

  /**
   * Azure SQL Data Warehouse-specific options.
   */
  sqldatawarehouse?: ISQLDataWarehouseOptions;
}

/**
 * Context methods are available when evaluating contextable SQL code, such as
 * within SQLX files, or when using a [Contextable](#Contextable) argument with the JS API.
 */
export interface ITableContext extends ICommonContext {
  /**
   * @hidden
   */
  isIncremental: () => boolean;

  /**
   * @hidden
   */
  ifIncremental: (value: string) => string;
}

/**
 * @hidden
 */
export class Table {
  public static readonly IGNORED_PROPS: {
    [tableType: string]: Array<keyof dataform.ITable>;
  } = {
    inline: [
      "bigquery",
      "redshift",
      "sqlDataWarehouse",
      "preOps",
      "postOps",
      "actionDescriptor",
      "disabled",
      "where"
    ]
  };

  public proto: dataform.ITable = dataform.Table.create({
    type: "view",
    disabled: false,
    tags: []
  });

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  public contextableQuery: Contextable<ITableContext, string>;
  private contextableWhere: Contextable<ITableContext, string>;
  private contextablePreOps: Array<Contextable<ITableContext, string | string[]>> = [];
  private contextablePostOps: Array<Contextable<ITableContext, string | string[]>> = [];

  public config(config: ITableConfig) {
    if (config.type) {
      this.type(config.type);
    }
    if (config.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (config.disabled) {
      this.disabled();
    }
    if (config.protected) {
      this.protected();
    }
    if (config.redshift) {
      this.redshift(config.redshift);
    }
    if (config.bigquery) {
      this.bigquery(config.bigquery);
    }
    if (config.sqldatawarehouse) {
      this.sqldatawarehouse(config.sqldatawarehouse);
    }
    if (config.tags) {
      this.tags(config.tags);
    }
    if (config.description) {
      this.description(config.description);
    }
    if (config.columns) {
      this.columns(config.columns);
    }
    if (config.database) {
      this.database(config.database);
    }
    if (config.schema) {
      this.schema(config.schema);
    }

    return this;
  }

  public type(type: TableType) {
    this.proto.type = type as string;
    return this;
  }

  public query(query: Contextable<ITableContext, string>) {
    this.contextableQuery = query;
    return this;
  }

  public where(where: Contextable<ITableContext, string>) {
    this.contextableWhere = where;
    return this;
  }

  public preOps(pres: Contextable<ITableContext, string | string[]>) {
    this.contextablePreOps.push(pres);
    return this;
  }

  public postOps(posts: Contextable<ITableContext, string | string[]>) {
    this.contextablePostOps.push(posts);
    return this;
  }

  public disabled() {
    this.proto.disabled = true;
    return this;
  }

  public protected() {
    this.proto.protected = true;
    return this;
  }

  public sqldatawarehouse(sqlDataWarehouse: dataform.ISQLDataWarehouseOptions) {
    this.proto.sqlDataWarehouse = dataform.SQLDataWarehouseOptions.create(sqlDataWarehouse);
    return this;
  }

  public redshift(redshift: dataform.IRedshiftOptions) {
    this.proto.redshift = dataform.RedshiftOptions.create(redshift);
    return this;
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    this.proto.bigquery = dataform.BigQueryOptions.create(bigquery);
    return this;
  }

  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable => {
      this.proto.dependencyTargets.push(utils.resolvableAsTarget(resolvable));
    });

    return this;
  }

  public tags(value: string | string[]) {
    const newTags = typeof value === "string" ? [value] : value;
    newTags.forEach(t => {
      this.proto.tags.push(t);
    });
    return this;
  }

  public description(description: string) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.description = description;
    return this;
  }

  public columns(columns: IColumnsDescriptor) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.columns = mapToColumnProtoArray(columns);
    return this;
  }

  public database(database: string) {
    utils.setNameAndTarget(
      this.session,
      this.proto,
      this.proto.target.name,
      this.proto.target.schema,
      database
    );
    return this;
  }

  public schema(schema: string) {
    utils.setNameAndTarget(
      this.session,
      this.proto,
      this.proto.target.name,
      schema,
      this.proto.target.database
    );
    return this;
  }

  public compile() {
    const context = new TableContext(this);
    const incrementalContext = new TableContext(this, true);

    this.proto.query = context.apply(this.contextableQuery);
    if (this.proto.type === "incremental") {
      this.proto.incrementalQuery = incrementalContext.apply(this.contextableQuery);
    }

    if (this.contextableWhere) {
      this.proto.where = context.apply(this.contextableWhere);
    }

    this.contextablePreOps.forEach(contextablePreOps => {
      const appliedPres = context.apply(contextablePreOps);
      this.proto.preOps = (this.proto.preOps || []).concat(
        typeof appliedPres === "string" ? [appliedPres] : appliedPres
      );
    });
    this.contextablePreOps = [];

    this.contextablePostOps.forEach(contextablePostOps => {
      const appliedPosts = context.apply(contextablePostOps);
      this.proto.postOps = (this.proto.postOps || []).concat(
        typeof appliedPosts === "string" ? [appliedPosts] : appliedPosts
      );
    });
    this.contextablePostOps = [];
    return this.proto;
  }
}

/**
 * @hidden
 */
export class TableContext implements ITableContext {
  constructor(private table: Table, private incremental = false) {}

  public config(config: ITableConfig) {
    this.table.config(config);
    return "";
  }

  public self(): string {
    return this.resolve(this.table.proto.target);
  }

  public name(): string {
    return this.table.proto.target.name;
  }

  public ref(ref: Resolvable | string[], ...rest: string[]): string {
    ref = utils.toResolvable(ref, rest);
    if (!utils.resolvableAsTarget(ref)) {
      const message = `Action name is not specified`;
      this.table.session.compileError(new Error(message));
      return "";
    }
    this.table.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.table.session.resolve(utils.toResolvable(ref, rest));
  }

  public type(type: TableType) {
    this.table.type(type);
    return "";
  }

  public where(where: Contextable<ITableContext, string>) {
    this.table.where(where);
    return "";
  }

  public isIncremental() {
    return !!this.incremental;
  }

  public ifIncremental(value: string) {
    return this.isIncremental() ? value : "";
  }

  public preOps(statement: Contextable<ITableContext, string | string[]>) {
    this.table.preOps(statement);
    return "";
  }

  public postOps(statement: Contextable<ITableContext, string | string[]>) {
    this.table.postOps(statement);
    return "";
  }

  public disabled() {
    this.table.disabled();
    return "";
  }

  public redshift(redshift: dataform.IRedshiftOptions) {
    this.table.redshift(redshift);
    return "";
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    this.table.bigquery(bigquery);
    return "";
  }

  public dependencies(res: Resolvable) {
    this.table.dependencies(res);
    return "";
  }

  public apply<T>(value: Contextable<ITableContext, T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }

  public tags(tags: string[]) {
    this.table.tags(tags);
    return "";
  }
}
