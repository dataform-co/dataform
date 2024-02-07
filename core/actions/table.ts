import { verifyObjectMatchesProto } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { Assertion } from "df/core/actions/assertion";
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
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  checkExcessProperties,
  nativeRequire,
  resolvableAsTarget,
  setNameAndTarget,
  strictKeysOf,
  tableTypeStringToEnum,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 */
export const TableType = ["table", "view", "incremental"] as const;
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

  /**
   * This setting specifies how long BigQuery keeps the data in each partition. The setting applies to all partitions in the table,
   * but is calculated independently for each partition based on the partition time.
   *
   * For more information, see our [docs](https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration).
   */
  partitionExpirationDays?: number;

  /**
   * When you create a partitioned table, you can require that all queries on the table must include a predicate filter (
   * a WHERE clause) that filters on the partitioning column.
   * This setting can improve performance and reduce costs,
   * because BigQuery can use the filter to prune partitions that don't match the predicate.
   *
   * For more information, see our [docs](https://cloud.google.com/bigquery/docs/managing-partitioned-tables#require-filter).
   */
  requirePartitionFilter?: boolean;

  /**
   * Key-value pairs for options [table](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list), [view](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#view_option_list), [materialized view](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#materialized_view_option_list).
   *
   * Some options (e.g. `partitionExpirationDays`) have dedicated type/validity checked fields; prefer using those.
   * String values need double-quotes, e.g. additionalOptions: {numeric_option: "5", string_option: '"string-value"'}
   * If the option name contains special characters, e.g. hyphens, then quote its name, e.g. additionalOptions: { "option-name": "value" }.
   */
  additionalOptions?: { [name: string]: string };
}

const IBigQueryOptionsProperties = () =>
  strictKeysOf<IBigQueryOptions>()([
    "partitionBy",
    "clusterBy",
    "updatePartitionFilter",
    "labels",
    "partitionExpirationDays",
    "requirePartitionFilter",
    "additionalOptions"
  ]);

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
   * BigQuery-specific warehouse options.
   */
  bigquery?: IBigQueryOptions;

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

  /**
   * Only valid when the table type is `view`.
   *
   * If set to true, will make the view materialized.
   *
   * For more information, read the [BigQuery materialized view docs](https://cloud.google.com/bigquery/docs/materialized-views-intro).
   */
  materialized?: boolean;
}

// TODO: This needs to be a method, I'm really not sure why, but it hits a runtime failure otherwise.
export const ITableConfigProperties = () =>
  strictKeysOf<ITableConfig>()([
    "type",
    "disabled",
    "protected",
    "name",
    "bigquery",
    "tags",
    "uniqueKey",
    "dependencies",
    "hermetic",
    "schema",
    "assertions",
    "database",
    "columns",
    "description",
    "materialized"
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
export class Table extends ActionBuilder<dataform.Table> {
  // TODO(ekrekr): make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.ITable = dataform.Table.create({
    type: "view",
    enumType: dataform.TableType.VIEW,
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

  constructor(
    session?: Session,
    tableTypeConfig?:
      | dataform.ActionConfig.TableConfig
      | dataform.ActionConfig.ViewConfig
      | dataform.ActionConfig.IncrementalTableConfig,
    // TODO(ekrekr): As part of JS API updates, instead of overloading, add new class files for the
    // view and incremental action types, instead of using this table type workaround.
    tableType?: TableType,
    configPath?: string
  ) {
    super(session);
    this.session = session;

    if (!tableTypeConfig) {
      return;
    }
    if (!tableType) {
      throw Error("Expected table type");
    }

    if (!tableTypeConfig.name) {
      tableTypeConfig.name = Path.fileName(tableTypeConfig.filename);
    }
    const target = actionConfigToCompiledGraphTarget(tableTypeConfig);
    this.proto.target = this.applySessionToTarget(target);
    this.proto.canonicalTarget = this.applySessionCanonicallyToTarget(target);

    // Resolve the filename as its absolute path.
    tableTypeConfig.filename = Path.join(Path.dirName(configPath), tableTypeConfig.filename);
    this.proto.fileName = tableTypeConfig.filename;

    // TODO(ekrekr): load config proto column descriptors.
    if (tableType === "table") {
      const config = tableTypeConfig as dataform.ActionConfig.TableConfig;

      // TODO(ekrekr): this is a workaround for avoiding keys that aren't present, and should be
      // cleaned up when the JS API is redone.
      const bigqueryOptions: IBigQueryOptions | undefined =
        config.partitionBy ||
        config.partitionExpirationDays ||
        config.requirePartitionFilter ||
        config.clusterBy.length ||
        Object.keys(config.labels).length ||
        Object.keys(config.additionalOptions).length
          ? {}
          : undefined;
      if (bigqueryOptions) {
        if (config.partitionBy) {
          bigqueryOptions.partitionBy = config.partitionBy;
        }
        if (config.partitionExpirationDays) {
          bigqueryOptions.partitionExpirationDays = config.partitionExpirationDays;
        }
        if (config.requirePartitionFilter) {
          bigqueryOptions.requirePartitionFilter = config.requirePartitionFilter;
        }
        if (config.clusterBy.length) {
          bigqueryOptions.clusterBy = config.clusterBy;
        }
        if (Object.keys(config.labels).length) {
          bigqueryOptions.labels = config.labels;
        }
        if (Object.keys(config.additionalOptions).length) {
          bigqueryOptions.additionalOptions = config.additionalOptions;
        }
      }

      this.config({
        type: "table",
        dependencies: config.dependencyTargets.map(dependencyTarget =>
          actionConfigToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
        ),
        tags: config.tags,
        disabled: config.disabled,
        description: config.description,
        bigquery: bigqueryOptions
      });
    }
    if (tableType === "view") {
      const config = tableTypeConfig as dataform.ActionConfig.ViewConfig;

      // TODO(ekrekr): this is a workaround for avoiding keys that aren't present, and should be
      // cleaned up when the JS API is redone.
      const bigqueryOptions: IBigQueryOptions | undefined =
        Object.keys(config.labels).length || Object.keys(config.additionalOptions).length
          ? {}
          : undefined;
      if (bigqueryOptions) {
        if (Object.keys(config.labels).length) {
          bigqueryOptions.labels = config.labels;
        }
        if (Object.keys(config.additionalOptions).length) {
          bigqueryOptions.additionalOptions = config.additionalOptions;
        }
      }

      this.config({
        type: "view",
        dependencies: config.dependencyTargets.map(dependencyTarget =>
          actionConfigToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
        ),
        disabled: config.disabled,
        materialized: config.materialized,
        tags: config.tags,
        description: config.description,
        bigquery: bigqueryOptions
      });
    }
    if (tableType === "incremental") {
      const config = tableTypeConfig as dataform.ActionConfig.IncrementalTableConfig;

      // TODO(ekrekr): this is a workaround for avoiding keys that aren't present, and should be
      // cleaned up when the JS API is redone.
      const bigqueryOptions: IBigQueryOptions | undefined =
        config.partitionBy ||
        config.partitionExpirationDays ||
        config.requirePartitionFilter ||
        config.updatePartitionFilter ||
        config.clusterBy.length ||
        Object.keys(config.labels).length ||
        Object.keys(config.additionalOptions).length
          ? {}
          : undefined;
      if (bigqueryOptions) {
        if (config.partitionBy) {
          bigqueryOptions.partitionBy = config.partitionBy;
        }
        if (config.partitionExpirationDays) {
          bigqueryOptions.partitionExpirationDays = config.partitionExpirationDays;
        }
        if (config.requirePartitionFilter) {
          bigqueryOptions.requirePartitionFilter = config.requirePartitionFilter;
        }
        if (config.updatePartitionFilter) {
          bigqueryOptions.updatePartitionFilter = config.updatePartitionFilter;
        }
        if (config.clusterBy.length) {
          bigqueryOptions.clusterBy = config.clusterBy;
        }
        if (Object.keys(config.labels).length) {
          bigqueryOptions.labels = config.labels;
        }
        if (Object.keys(config.additionalOptions).length) {
          bigqueryOptions.additionalOptions = config.additionalOptions;
        }
      }

      this.config({
        type: "incremental",
        dependencies: config.dependencyTargets.map(dependencyTarget =>
          actionConfigToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
        ),
        disabled: config.disabled,
        protected: config.protected,
        uniqueKey: config.uniqueKey,
        tags: config.tags,
        description: config.description,
        bigquery: bigqueryOptions
      });
    }
    this.query(nativeRequire(tableTypeConfig.filename).query);
    if (tableTypeConfig.preOperations) {
      this.preOps(tableTypeConfig.preOperations);
    }
    if (tableTypeConfig.postOperations) {
      this.postOps(tableTypeConfig.postOperations);
    }
  }

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
    if (config.bigquery && Object.keys(config.bigquery).length > 0) {
      this.bigquery(config.bigquery);
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
    if (config.materialized) {
      this.materialized(config.materialized);
    }

    return this;
  }

  public type(type: TableType) {
    this.proto.type = type;
    this.proto.enumType = tableTypeStringToEnum(type, false);
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

  public materialized(materialized: boolean) {
    this.proto.materialized = materialized;
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
    this.uniqueKeyAssertions.forEach(assertion => assertion.tags(value));
    this.rowConditionsAssertion?.tags(value);
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
          ctx => this.session.compilationSql().indexAssertion(ctx.ref(this.proto.target), uniqueKey)
        );
        if (this.proto.tags) {
          uniqueKeyAssertion.tags(this.proto.tags);
        }
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
            .compilationSql()
            .rowConditionsAssertion(ctx.ref(this.proto.target), mergedRowConditions)
      );
      this.rowConditionsAssertion.proto.parentAction = this.proto.target;
      if (this.proto.disabled) {
        this.rowConditionsAssertion.disabled();
      }
      if (this.proto.tags) {
        this.rowConditionsAssertion.tags(this.proto.tags);
      }
    }
    return this;
  }

  /**
   * @hidden
   */
  public getFileName() {
    return this.proto.fileName;
  }

  /**
   * @hidden
   */
  public getTarget() {
    return dataform.Target.create(this.proto.target);
  }

  public compile() {
    const context = new TableContext(this);
    const incrementalContext = new TableContext(this, true);

    this.proto.query = context.apply(this.contextableQuery);

    if (this.proto.enumType === dataform.TableType.INCREMENTAL) {
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

    this.proto.preOps = this.contextifyOps(this.contextablePreOps, context).filter(
      op => !!op.trim()
    );
    this.proto.postOps = this.contextifyOps(this.contextablePostOps, context).filter(
      op => !!op.trim()
    );

    validateQueryString(this.session, this.proto.query, this.proto.fileName);
    validateQueryString(this.session, this.proto.incrementalQuery, this.proto.fileName);

    return verifyObjectMatchesProto(dataform.Table, this.proto);
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
    return this.table.session.finalizeName(this.table.proto.target.name);
  }

  public ref(ref: Resolvable | string[], ...rest: string[]): string {
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      this.table.session.compileError(new Error(`Action name is not specified`));
      return "";
    }
    this.table.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.table.session.resolve(ref, ...rest);
  }

  public schema(): string {
    return this.table.session.finalizeSchema(this.table.proto.target.schema);
  }

  public database(): string {
    if (!this.table.proto.target.database) {
      this.table.session.compileError(new Error(`Warehouse does not support multiple databases`));
      return "";
    }

    return this.table.session.finalizeDatabase(this.table.proto.target.database);
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
