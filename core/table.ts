import { Assertion } from "df/core/assertion";
import { ColumnDescriptors } from "df/core/column_descriptors";
import {
  Contextable,
  IActionConfig,
  IColumnsDescriptor,
  ICommonContext,
  IDependenciesConfig,
  IDocumentableConfig,
  INamedConfig,
  ITargetableConfig,
  Resolvable
} from "df/core/common";
import { Session } from "df/core/session";
import {
  checkExcessProperties,
  resolvableAsTarget,
  setNameAndTarget,
  strictKeysOf,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";

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
 * Redshift-specific warehouse options.
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
}

const IRedshiftOptionsProperties = () =>
  strictKeysOf<IRedshiftOptions>()(["distKey", "distStyle", "sortKeys", "sortStyle"]);

/**
 * Snowflake-specific warehouse options.
 */
export interface ISnowflakeOptions {
  /**
   * If set to true, a secure view will be created.
   *
   * For more information, read the [Snowflake Secure Views docs](https://docs.snowflake.com/en/user-guide/views-secure.html).
   */
  secure?: boolean;

  /**
   * If set to true, a transient table will be created. Only applicable to actions of type "table".
   *
   * For more information, read the [Snowflake docs](https://docs.snowflake.com/en/user-guide/tables-temp-transient.html).
   */
  transient?: boolean;

  /**
   * A list of clustering keys to cluster the table by. Only applicable to actions of type "table" or "incremental".
   *
   * For more information, read the [Snowflake clustering docs](https://docs.snowflake.com/en/user-guide/tables-clustering-keys.html).
   */
  clusterBy?: string[];
}
const ISnowflakeOptionsProperties = () =>
  strictKeysOf<ISnowflakeOptions>()(["secure", "transient", "clusterBy"]);

/**
 * Azure SQL Data Warehouse-specific warehouse options.
 */
export interface ISQLDataWarehouseOptions {
  /**
   * The distribution option value.
   *
   * For more information, read the [Azure CTAS docs](https://docs.microsoft.com/en-gb/sql/t-sql/statements/create-table-as-select-azure-sql-data-warehouse?view=aps-pdw-2016-au7#examples-for-table-distribution).
   */
  distribution?: string;
}
const ISQLDataWarehouseOptionsProperties = () =>
  strictKeysOf<ISQLDataWarehouseOptions>()(["distribution"]);

/**
 * BigQuery-specific warehouse options.
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

  /**
   * SQL based filter for when incremental updates are applied.
   *
   * For more information, see our [incremental dataset docs](https://docs.dataform.co/guides/incremental-datasets).
   */
  updatePartitionFilter?: string;

  /**
   * Key-value pairs for [BigQuery labels](https://cloud.google.com/bigquery/docs/labels-intro).
   *
   * If the label name contains special characters, e.g. hyphens, then quote its name, e.g. labels: { "label-name": "value" }.
   */
  labels?: { [name: string]: string };
}

const IBigQueryOptionsProperties = () =>
  strictKeysOf<IBigQueryOptions>()(["partitionBy", "clusterBy", "updatePartitionFilter", "labels"]);

/**
 * Options for creating tables within Presto projects.
 */
export interface IPrestoOptions {
  /**
   * The key with which to partition the table. Typically the name of a timestamp or date column.
   *
   * For more information, read the partitioning documentation for the Presto connection in use.
   */
  partitionBy?: string[];
}

const IPrestoOptionsProperties = () => strictKeysOf<IPrestoOptions>()(["partitionBy"]);

/**
 * Options for creating assertions as part of a dataset definition.
 */
export interface ITableAssertions {
  /**
   * Column(s) which constitute the dataset's unique key index.
   *
   * If set, the resulting assertion will fail if there is more than one row in the dataset with the same values for all of these column(s).
   */
  uniqueKey?: string | string[];

  /**
   * Combinations of column(s), each of which should constitute a unique key index for the dataset.
   *
   * If set, the resulting assertion(s) will fail if there is more than one row in the dataset with the same values for all of the column(s)
   * in the unique key(s).
   */
  uniqueKeys?: string[][];

  /**
   * Column(s) which may never be `NULL`.
   *
   * If set, the resulting assertion will fail if any row contains `NULL` values for these column(s).
   */
  nonNull?: string | string[];

  /**
   * General condition(s) which should hold true for all rows in the dataset.
   *
   * If set, the resulting assertion will fail if any row violates any of these condition(s).
   */
  rowConditions?: string[];
}

const ITableAssertionsProperties = () =>
  strictKeysOf<ITableAssertions>()(["uniqueKey", "uniqueKeys", "nonNull", "rowConditions"]);

/**
 * Configuration options for `dataset` actions, including `table`, `view` and `incremental` action types.
 */
export interface ITableConfig
  extends IActionConfig,
    IDependenciesConfig,
    IDocumentableConfig,
    INamedConfig,
    ITargetableConfig {
  /**
   * The type of the dataset. For more information on how this setting works, check out some of the [guides](guides)
   * on publishing different types of datasets with Dataform.
   */
  type?: TableType;

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
   * Snowflake-specific options.
   */
  snowflake?: ISnowflakeOptions;

  /**
   * Azure SQL Data Warehouse-specific options.
   */
  sqldatawarehouse?: ISQLDataWarehouseOptions;

  /**
   * Presto-specific options.
   */
  presto?: IPrestoOptions;

  /**
   * Assertions to be run on the dataset.
   *
   * If configured, relevant assertions will automatically be created and run as a dependency of this dataset.
   */
  assertions?: ITableAssertions;

  /**
   * Unique keys for merge criteria for incremental tables.
   *
   * If configured, records with matching unique key(s) will be updated, rather than new rows being inserted.
   */
  uniqueKey?: string[];
}

// TODO: This needs to be a method, I'm really not sure why, but it hits a runtime failure otherwise.
export const ITableConfigProperties = () =>
  strictKeysOf<ITableConfig>()([
    "type",
    "disabled",
    "protected",
    "name",
    "redshift",
    "bigquery",
    "snowflake",
    "sqldatawarehouse",
    "presto",
    "tags",
    "uniqueKey",
    "dependencies",
    "hermetic",
    "schema",
    "assertions",
    "database",
    "columns",
    "description"
  ]);

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
      "snowflake",
      "sqlDataWarehouse",
      "presto",
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

  private uniqueKeyAssertions: Assertion[] = [];
  private rowConditionsAssertion: Assertion;

  public config(config: ITableConfig) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      config,
      ITableConfigProperties(),
      "table config"
    );
    if (config.type) {
      this.type(config.type);
    }
    if (config.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (config.hermetic !== undefined) {
      this.hermetic(config.hermetic);
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
    if (config.snowflake) {
      this.snowflake(config.snowflake);
    }
    if (config.sqldatawarehouse) {
      this.sqldatawarehouse(config.sqldatawarehouse);
    }
    if (config.presto) {
      this.presto(config.presto);
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
    if (config.uniqueKey) {
      this.uniqueKey(config.uniqueKey);
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
    this.uniqueKeyAssertions.forEach(assertion => assertion.disabled());
    this.rowConditionsAssertion?.disabled();
    return this;
  }

  public protected() {
    this.proto.protected = true;
    return this;
  }

  public uniqueKey(uniqueKey: string[]) {
    this.proto.uniqueKey = uniqueKey;
  }

  public snowflake(snowflake: dataform.ISnowflakeOptions) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      snowflake,
      ISnowflakeOptionsProperties(),
      "snowflake config"
    );
    this.proto.snowflake = dataform.SnowflakeOptions.create(snowflake);
    return this;
  }

  public sqldatawarehouse(sqlDataWarehouse: dataform.ISQLDataWarehouseOptions) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      sqlDataWarehouse,
      ISQLDataWarehouseOptionsProperties(),
      "sqldatawarehouse config"
    );
    this.proto.sqlDataWarehouse = dataform.SQLDataWarehouseOptions.create(sqlDataWarehouse);
    return this;
  }

  public redshift(redshift: dataform.IRedshiftOptions) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      redshift,
      IRedshiftOptionsProperties(),
      "redshift config"
    );
    this.proto.redshift = dataform.RedshiftOptions.create(redshift);
    return this;
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      bigquery,
      IBigQueryOptionsProperties(),
      "bigquery config"
    );
    this.proto.bigquery = dataform.BigQueryOptions.create(bigquery);
    if (!!bigquery.labels) {
      if (!this.proto.actionDescriptor) {
        this.proto.actionDescriptor = {};
      }
      this.proto.actionDescriptor.bigqueryLabels = bigquery.labels;
    }
    return this;
  }

  public presto(presto: dataform.IPrestoOptions) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      presto,
      IPrestoOptionsProperties(),
      "presto config"
    );
    this.proto.presto = dataform.PrestoOptions.create(presto);
    return this;
  }

  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable => {
      this.proto.dependencyTargets.push(resolvableAsTarget(resolvable));
    });

    return this;
  }

  public hermetic(hermetic: boolean) {
    this.proto.hermeticity = hermetic
      ? dataform.ActionHermeticity.HERMETIC
      : dataform.ActionHermeticity.NON_HERMETIC;
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
    this.proto.actionDescriptor.columns = ColumnDescriptors.mapToColumnProtoArray(
      columns,
      (e: Error) => this.session.compileError(e)
    );
    return this;
  }

  public database(database: string) {
    setNameAndTarget(
      this.session,
      this.proto,
      this.proto.target.name,
      this.proto.target.schema,
      database
    );
    return this;
  }

  public schema(schema: string) {
    setNameAndTarget(
      this.session,
      this.proto,
      this.proto.target.name,
      schema,
      this.proto.target.database
    );
    return this;
  }

  public assertions(assertions: ITableAssertions) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      assertions,
      ITableAssertionsProperties(),
      "assertions config"
    );
    if (!!assertions.uniqueKey && !!assertions.uniqueKeys) {
      this.session.compileError(
        new Error("Specify at most one of 'assertions.uniqueKey' and 'assertions.uniqueKeys'.")
      );
    }
    let uniqueKeys = assertions.uniqueKeys;
    if (!!assertions.uniqueKey) {
      uniqueKeys =
        typeof assertions.uniqueKey === "string"
          ? [[assertions.uniqueKey]]
          : [assertions.uniqueKey];
    }
    if (uniqueKeys) {
      uniqueKeys.forEach((uniqueKey, index) => {
        const uniqueKeyAssertion = this.session.assert(
          `${this.proto.target.schema}_${this.proto.target.name}_assertions_uniqueKey_${index}`,
          ctx => this.session.adapter().indexAssertion(ctx.ref(this.proto.target), uniqueKey)
        );
        uniqueKeyAssertion.proto.parentAction = this.proto.target;
        if (this.proto.disabled) {
          uniqueKeyAssertion.disabled();
        }
        this.uniqueKeyAssertions.push(uniqueKeyAssertion);
      });
    }
    const mergedRowConditions = assertions.rowConditions || [];
    if (!!assertions.nonNull) {
      const nonNullCols =
        typeof assertions.nonNull === "string" ? [assertions.nonNull] : assertions.nonNull;
      nonNullCols.forEach(nonNullCol => mergedRowConditions.push(`${nonNullCol} IS NOT NULL`));
    }
    if (!!mergedRowConditions && mergedRowConditions.length > 0) {
      this.rowConditionsAssertion = this.session.assert(
        `${this.proto.target.schema}_${this.proto.target.name}_assertions_rowConditions`,
        ctx =>
          this.session
            .adapter()
            .rowConditionsAssertion(ctx.ref(this.proto.target), mergedRowConditions)
      );
      this.rowConditionsAssertion.proto.parentAction = this.proto.target;
      if (this.proto.disabled) {
        this.rowConditionsAssertion.disabled();
      }
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

    validateQueryString(this.session, this.proto.query);
    this.proto.preOps.forEach(preOp => validateQueryString(this.session, preOp));
    this.proto.postOps.forEach(postOp => validateQueryString(this.session, postOp));

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
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      const message = `Action name is not specified`;
      this.table.session.compileError(new Error(message));
      return "";
    }
    this.table.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.table.session.resolve(toResolvable(ref, rest));
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

  public presto(presto: dataform.IPrestoOptions) {
    this.table.presto(presto);
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
