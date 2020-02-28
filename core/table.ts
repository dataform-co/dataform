import { IAdapter } from "@dataform/core/adapters";
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
import { AssertionContext } from "df/core/assertion";

/**
 * @hidden
 */
export const TableType = ["table", "view", "incremental", "inline"] as const;
/**
 * Supported types of table actions.
 *
 * Tables of type `view` will be created as views.
 *
 * Tables of type `table` will be created as tables.
 *
 * Tables of type `incremental` must have a where clause provided. For more information, see the [incremental tables guide](guides/incremental-datasets).
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

  /**
   * The keys by which to cluster partitions by.
   *
   * For more information, read the [BigQuery clustered tables docs](https://cloud.google.com/bigquery/docs/clustered-tables).
   */
  clusterBy?: string[];
}

/**
 * Configuration options for `dataset` actions, including `table`, `view` and `incremental` action types.
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

  /**
   * Assertions to be run on the dataset.
   *
   * If configured, relevant assertions will automatically be created and run as a dependency of this dataset.
   */
  assertions?: {
    /**
     * Column(s) which constitute the dataset's index.
     *
     * If set, the resulting assertion will fail if there is more than one row in the dataset with the same values for all of these column(s).
     */
    index?: string | string[];

    /**
     * Column(s) which may never be `NULL`.
     *
     * If set, the resulting assertion will fail if any row contains `NULL` values for these column(s).
     */
    nonNull?: string[];

    /**
     * General condition(s) which should hold true for all rows in the dataset.
     *
     * If set, the resulting assertion will fail if any row violates any of these condition(s).
     */
    rowConditions?: string[];
  };
}

/**
 * Context methods are available when evaluating contextable SQL code, such as
 * within SQLX files, or when using a [Contextable](#Contextable) argument with the JS API.
 */
export interface ITableContext extends ICommonContext {
  /**
   * Shorthand for an `if` condition. Equivalent to `cond ? trueCase : falseCase`.
   * `falseCase` is optional, and defaults to an empty string.
   */
  when: (cond: boolean, trueCase: string, falseCase?: string) => string;

  /**
   * Indicates whether the config indicates the file is dealing with an incremental table.
   */
  incremental: () => boolean;
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
    if (config.assertions) {
      this.assertions(config.assertions);
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

  public assertions({
    index,
    nonNull,
    rowConditions
  }: {
    index?: string | string[];
    nonNull?: string[];
    rowConditions?: string[];
  }) {
    if (!!index) {
      const indexCols = typeof index === "string" ? [index] : index;
      this.session.assert(`${this.proto.target.name}_assertions_index`, ctx =>
        indexAssertionSql(ctx, this.proto.target, indexCols)
      );
    }
    const mergedRowConditions = rowConditions || [];
    if (!!nonNull) {
      nonNull.forEach(nonNullCol => mergedRowConditions.push(`${nonNullCol} IS NOT NULL`));
    }
    if (!!mergedRowConditions) {
      this.session.assert(`${this.proto.target.name}_assertions_rowConditions`, ctx =>
        rowConditionsAssertionSql(
          ctx,
          this.session.adapter(),
          this.proto.target,
          mergedRowConditions
        )
      );
    }
    return this;
  }

  public compile() {
    const context = new TableContext(this);
    const incrementalContext = new TableContext(this, true);

    this.proto.query = context.apply(this.contextableQuery);

    if (this.proto.type === "incremental") {
      this.proto.incrementalQuery = incrementalContext.apply(this.contextableQuery);

      this.proto.incrementalPreOps = this.contextifyOps(this.contextablePreOps, incrementalContext);
      this.proto.incrementalPostOps = this.contextifyOps(
        this.contextablePostOps,
        incrementalContext
      );
    }

    if (this.contextableWhere) {
      this.proto.where = context.apply(this.contextableWhere);
    }

    this.proto.preOps = this.contextifyOps(this.contextablePreOps, context);
    this.proto.postOps = this.contextifyOps(this.contextablePostOps, context);

    return this.proto;
  }

  private contextifyOps(
    contextableOps: Array<Contextable<ITableContext, string | string[]>>,
    currentContext: TableContext
  ) {
    let protoOps: string[] = [];
    contextableOps.forEach(contextableOp => {
      const appliedOps = currentContext.apply(contextableOp);
      protoOps = protoOps.concat(typeof appliedOps === "string" ? [appliedOps] : appliedOps);
    });
    return protoOps;
  }
}

/**
 * @hidden
 */
export class TableContext implements ITableContext {
  constructor(private table: Table, private isIncremental = false) {}

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

  public when(cond: boolean, trueCase: string, falseCase: string = "") {
    return cond ? trueCase : falseCase;
  }

  public incremental() {
    return !!this.isIncremental;
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

function indexAssertionSql(ctx: AssertionContext, table: dataform.ITarget, indexCols: string[]) {
  const commaSeparatedColumns = indexCols.join(", ");
  return `
SELECT
  *
FROM (
  SELECT
    COUNT(1) AS index_row_count,
    ${commaSeparatedColumns}
  FROM ${ctx.ref(table)}
  GROUP BY ${commaSeparatedColumns}
  ) AS data
WHERE index_row_count > 1
`;
}

function rowConditionsAssertionSql(
  ctx: AssertionContext,
  adapter: IAdapter,
  table: dataform.ITarget,
  rowConditions: string[]
) {
  return rowConditions
    .map(
      (rowCondition: string) => `
SELECT
  ${adapter.sqlString(rowCondition)} AS failing_row_condition,
  *
FROM ${ctx.ref(table)}
WHERE NOT (${rowCondition})
`
    )
    .join(`UNION ALL`);
}
