import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
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
  addDependenciesToActionDependencyTargets,
  checkExcessProperties,
  nativeRequire,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  setNameAndTarget,
  strictKeysOf,
  tableTypeStringToEnum,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";

export const TableType = ["table", "view", "incremental"] as const;
export type TableType = typeof TableType[number];

export interface LegacyTableConfig extends dataform.ActionConfig.TableConfig {
  type?: TableType;
  // TODO: move these assertions fields to the configs proto.
  assertions?: {
    uniqueKey?: string | string[];
    uniqueKeys?: string[][];
    nonNull?: string | string[];
    rowConditions?: string[];
  };
}

export interface LegacyIncrementalTableConfig extends dataform.ActionConfig.IncrementalTableConfig {
  type?: TableType;
}

export interface LegacyViewConfig extends dataform.ActionConfig.ViewConfig {
  type?: TableType;
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

  // If true, adds the inline assertions of dependencies as direct dependencies for this action.
  public dependOnDependencyAssertions: boolean = false;

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
      tableTypeConfig.name = Path.basename(tableTypeConfig.filename);
    }
    const target = actionConfigToCompiledGraphTarget(tableTypeConfig);
    this.proto.target = this.applySessionToTarget(
      target,
      session.projectConfig,
      tableTypeConfig.filename,
      true
    );
    this.proto.canonicalTarget = this.applySessionToTarget(target, session.canonicalProjectConfig);

    tableTypeConfig.filename = resolveActionsConfigFilename(tableTypeConfig.filename, configPath);
    this.proto.fileName = tableTypeConfig.filename;

    // TODO(ekrekr): load config proto column descriptors.
    if (tableType === "table") {
      const config = tableTypeConfig as dataform.ActionConfig.TableConfig;

      // TODO(ekrekr): this is a workaround for avoiding keys that aren't present, and should be
      // cleaned up when the JS API is redone.
      const bigqueryOptions: any =
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
        ...config
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
        bigquery: bigqueryOptions,
        dependOnDependencyAssertions: config.dependOnDependencyAssertions
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
        bigquery: bigqueryOptions,
        dependOnDependencyAssertions: config.dependOnDependencyAssertions
      });
    }
    this.query(nativeRequire(tableTypeConfig.filename).query);
    if (tableTypeConfig.preOperations) {
      this.preOps(tableTypeConfig.preOperations);
    }
    if (tableTypeConfig.postOperations) {
      this.postOps(tableTypeConfig.postOperations);
    }
    if (tableTypeConfig.dependOnDependencyAssertions) {
      this.setDependOnDependencyAssertions(tableTypeConfig.dependOnDependencyAssertions);
    }
    if (tableTypeConfig.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (tableTypeConfig.hermetic !== undefined) {
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

  public disabled(disabled = true) {
    this.proto.disabled = disabled;
    this.uniqueKeyAssertions.forEach(assertion => assertion.disabled(disabled));
    this.rowConditionsAssertion?.disabled(disabled);
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

  public dependencies(value: dataform.ActionConfig.Target[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable =>
      addDependenciesToActionDependencyTargets(this, resolvable)
    );
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

  public setDependOnDependencyAssertions(dependOnDependencyAssertions: boolean) {
    this.dependOnDependencyAssertions = dependOnDependencyAssertions;
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

    return verifyObjectMatchesProto(
      dataform.Table,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
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
