import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import {
  ActionBuilder,
  ILegacyTableBigqueryConfig,
  ITableContext,
  LegacyConfigConverter,
  TableType
} from "df/core/actions";
import { Assertion } from "df/core/actions/assertion";
import { ColumnDescriptors } from "df/core/column_descriptors";
import { Contextable, Resolvable } from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  addDependenciesToActionDependencyTargets,
  checkExcessProperties,
  configTargetToCompiledGraphTarget,
  nativeRequire,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  setNameAndTarget,
  strictKeysOf,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";
import { View } from "df/core/actions/view";
import { Table } from "df/core/actions/table";

/**
 * @hidden
 * This maintains backwards compatability with older versions.
 * TODO(ekrekr): consider breaking backwards compatability of these in v4.
 */
export interface ILegacyIncrementalTableConfig
  extends dataform.ActionConfig.IncrementalTableConfig {
  dependencies: Resolvable[];
  database: string;
  schema: string;
  fileName: string;
  type: string;
  bigquery?: ILegacyTableBigqueryConfig;
  // Legacy incremental table config's table assertions cannot directly extend the protobuf
  // incremental table config definition because of legacy incremental table config's flexible
  // types.
  assertions: any;
}

/**
 * @hidden
 */
export class IncrementalTable extends ActionBuilder<dataform.Table> {
  // TODO(ekrekr): make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.ITable = dataform.Table.create({
    type: "incremental",
    enumType: dataform.TableType.INCREMENTAL,
    disabled: false,
    tags: []
  });

  private unverifiedConfig: any;
  private configPath: string | undefined;

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

  constructor(session?: Session, unverifiedConfig?: any, configPath?: string) {
    super(session);
    this.session = session;
    this.unverifiedConfig = unverifiedConfig;
    this.configPath = configPath;

    if (!unverifiedConfig) {
      return;
    }

    const config = this.verifyConfig(unverifiedConfig);

    if (!config.name) {
      config.name = Path.basename(config.filename);
    }
    const target = actionConfigToCompiledGraphTarget(config);
    this.proto.target = this.applySessionToTarget(
      target,
      session.projectConfig,
      config.filename,
      true
    );
    this.proto.canonicalTarget = this.applySessionToTarget(target, session.canonicalProjectConfig);

    if (configPath) {
      config.filename = resolveActionsConfigFilename(config.filename, configPath);
      this.query(nativeRequire(config.filename).query);
    }

    if (config.dependOnDependencyAssertions) {
      this.setDependOnDependencyAssertions(config.dependOnDependencyAssertions);
    }
    if (config.dependencyTargets) {
      this.dependencies(
        config.dependencyTargets.map(dependencyTarget =>
          configTargetToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
        )
      );
    }
    if (config.hermetic !== undefined) {
      this.hermetic(config.hermetic);
    }
    if (config.disabled) {
      this.disabled();
    }
    if (config.tags) {
      this.tags(config.tags);
    }
    if (config.description) {
      this.description(config.description);
    }
    if (config.columns?.length) {
      this.columns(
        config.columns.map(columnDescriptor =>
          dataform.ActionConfig.ColumnDescriptor.create(columnDescriptor)
        )
      );
    }
    if (config.project) {
      this.database(config.project);
    }
    if (config.dataset) {
      this.schema(config.dataset);
    }
    if (config.assertions) {
      this.assertions(dataform.ActionConfig.TableAssertionsConfig.create(config.assertions));
    }
    if (config.uniqueKey) {
      this.uniqueKey(config.uniqueKey);
    }
    this.protected(config.protected);
    if (config.preOperations) {
      this.preOps(config.preOperations);
    }
    if (config.postOperations) {
      this.postOps(config.postOperations);
    }
    this.bigquery({
      partitionBy: config.partitionBy,
      clusterBy: config.clusterBy,
      updatePartitionFilter: config.updatePartitionFilter,
      labels: config.labels,
      partitionExpirationDays: config.partitionExpirationDays,
      requirePartitionFilter: config.requirePartitionFilter,
      additionalOptions: config.additionalOptions
    });
    if (config.filename) {
      this.proto.fileName = config.filename;
    }
    this.proto.onSchemaChange = this.mapOnSchemaChange(config.onSchemaChange);

    return this;
  }

  /**
   * @hidden
   * @deprecated
   * Deprecated in favor of action type can being set in the configs passed to action constructor
   * functions.
   */
  public type(type: TableType) {
    if (type === "table") {
      return new Table(this.session, this.unverifiedConfig, this.configPath);
    }
    if (type === "incremental") {
      return new IncrementalTable(this.session, this.unverifiedConfig, this.configPath);
    }
    if (type === "view") {
      return new View(this.session, this.unverifiedConfig, this.configPath);
    }
    throw new Error(`Unexpected table type: ${type}`);
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

  public protected(isProtected: boolean) {
    this.proto.protected = isProtected;
    return this;
  }

  public uniqueKey(uniqueKey: string[]) {
    this.proto.uniqueKey = uniqueKey;
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    if (!!bigquery.labels && Object.keys(bigquery.labels).length > 0) {
      if (!this.proto.actionDescriptor) {
        this.proto.actionDescriptor = {};
      }
      this.proto.actionDescriptor.bigqueryLabels = bigquery.labels;
    }

    const bigqueryFiltered = LegacyConfigConverter.legacyConvertBigQueryOptions(bigquery);
    if (Object.values(bigqueryFiltered).length > 0) {
      this.proto.bigquery = dataform.BigQueryOptions.create(bigqueryFiltered);
    }
    return this;
  }

  public dependencies(value: Resolvable | Resolvable[]) {
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

  public columns(columns: dataform.ActionConfig.ColumnDescriptor[]) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.columns = ColumnDescriptors.mapConfigProtoToCompilationProto(
      columns
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

  public assertions(assertions: dataform.ActionConfig.TableAssertionsConfig) {
    if (!!assertions.uniqueKey?.length && !!assertions.uniqueKeys?.length) {
      this.session.compileError(
        new Error("Specify at most one of 'assertions.uniqueKey' and 'assertions.uniqueKeys'.")
      );
    }
    let uniqueKeys = assertions.uniqueKeys.map(uniqueKey =>
      dataform.ActionConfig.TableAssertionsConfig.UniqueKey.create(uniqueKey)
    );
    if (!!assertions.uniqueKey?.length) {
      uniqueKeys = [
        dataform.ActionConfig.TableAssertionsConfig.UniqueKey.create({
          uniqueKey: assertions.uniqueKey
        })
      ];
    }
    if (uniqueKeys) {
      uniqueKeys.forEach(({ uniqueKey }, index) => {
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
    const context = new IncrementalTableContext(this);
    const incrementalContext = new IncrementalTableContext(this, true);

    this.proto.query = context.apply(this.contextableQuery);

    this.proto.incrementalQuery = incrementalContext.apply(this.contextableQuery);

    this.proto.incrementalPreOps = this.contextifyOps(this.contextablePreOps, incrementalContext);
    this.proto.incrementalPostOps = this.contextifyOps(this.contextablePostOps, incrementalContext);

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
    currentContext: IncrementalTableContext
  ) {
    let protoOps: string[] = [];
    contextableOps.forEach(contextableOp => {
      const appliedOps = currentContext.apply(contextableOp);
      protoOps = protoOps.concat(typeof appliedOps === "string" ? [appliedOps] : appliedOps);
    });
    return protoOps;
  }

  private verifyConfig(
    unverifiedConfig: ILegacyIncrementalTableConfig
  ): dataform.ActionConfig.IncrementalTableConfig {
    // The "type" field only exists on legacy incremental table configs. Here we convert them to the
    // new format.
    if (unverifiedConfig.type) {
      delete unverifiedConfig.type;
      if (unverifiedConfig.dependencies) {
        unverifiedConfig.dependencyTargets = unverifiedConfig.dependencies.map(
          (dependency: string | object) =>
            typeof dependency === "string" ? { name: dependency } : dependency
        );
        delete unverifiedConfig.dependencies;
      }
      if (unverifiedConfig.database) {
        unverifiedConfig.project = unverifiedConfig.database;
        delete unverifiedConfig.database;
      }
      if (unverifiedConfig.schema) {
        unverifiedConfig.dataset = unverifiedConfig.schema;
        delete unverifiedConfig.schema;
      }
      if (unverifiedConfig.fileName) {
        unverifiedConfig.filename = unverifiedConfig.fileName;
        delete unverifiedConfig.fileName;
      }
      if (unverifiedConfig.columns) {
        unverifiedConfig.columns = ColumnDescriptors.mapLegacyObjectToConfigProto(
          unverifiedConfig.columns as any
        );
      }
      unverifiedConfig = LegacyConfigConverter.insertLegacyInlineAssertionsToConfigProto(
        unverifiedConfig
      );
      unverifiedConfig = LegacyConfigConverter.insertLegacyBigQueryOptionsToConfigProto(
        unverifiedConfig
      );
      if (unverifiedConfig.bigquery) {
        checkExcessProperties(
          (e: Error) => {
            throw e;
          },
          unverifiedConfig.bigquery,
          strictKeysOf<ILegacyTableBigqueryConfig>()([
            "partitionBy",
            "clusterBy",
            "updatePartitionFilter",
            "labels",
            "partitionExpirationDays",
            "requirePartitionFilter",
            "additionalOptions"
          ]),
          "BigQuery table config"
        );
      }

      // To prevent accidental data deletion, protected defaults to true if unspecified.
      if (unverifiedConfig.protected === undefined || unverifiedConfig.protected === null) {
        unverifiedConfig.protected = true;
      }
    }

    return verifyObjectMatchesProto(
      dataform.ActionConfig.IncrementalTableConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    );
  }

  // The type of onSchemaChange depends on the source file:
  // - for sqlx it will have type "string"
  // - for action.yaml it will be converted to enum which is represented
  // in TypeScript as a "number".
  private mapOnSchemaChange(onSchemaChange?: string | number): dataform.OnSchemaChange {
    if (!onSchemaChange) {
      return dataform.OnSchemaChange.IGNORE;
    }

    if (typeof onSchemaChange === "number") {
      switch (onSchemaChange) {
        case dataform.ActionConfig.OnSchemaChange.IGNORE:
          return dataform.OnSchemaChange.IGNORE;
        case dataform.ActionConfig.OnSchemaChange.FAIL:
          return dataform.OnSchemaChange.FAIL;
        case dataform.ActionConfig.OnSchemaChange.EXTEND:
          return dataform.OnSchemaChange.EXTEND;
        case dataform.ActionConfig.OnSchemaChange.SYNCHRONIZE:
          return dataform.OnSchemaChange.SYNCHRONIZE;
        default:
          throw new Error(`OnSchemaChange value "${onSchemaChange}" is not supported`);
      }
    }

    switch (onSchemaChange.toString().toUpperCase()) {
      case "IGNORE":
        return dataform.OnSchemaChange.IGNORE;
      case "FAIL":
        return dataform.OnSchemaChange.FAIL;
      case "EXTEND":
        return dataform.OnSchemaChange.EXTEND;
      case "SYNCHRONIZE":
        return dataform.OnSchemaChange.SYNCHRONIZE;
      default:
        throw new Error(`OnSchemaChange value "${onSchemaChange}" is not supported`);
    }
  }
}

/**
 * @hidden
 */
export class IncrementalTableContext implements ITableContext {
  constructor(private incrementalTable: IncrementalTable, private isIncremental = false) {}

  public self(): string {
    return this.resolve(this.incrementalTable.proto.target);
  }

  public name(): string {
    return this.incrementalTable.session.finalizeName(this.incrementalTable.proto.target.name);
  }

  public ref(ref: Resolvable | string[], ...rest: string[]): string {
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      this.incrementalTable.session.compileError(new Error(`Action name is not specified`));
      return "";
    }
    this.incrementalTable.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.incrementalTable.session.resolve(ref, ...rest);
  }

  public schema(): string {
    return this.incrementalTable.session.finalizeSchema(this.incrementalTable.proto.target.schema);
  }

  public database(): string {
    if (!this.incrementalTable.proto.target.database) {
      this.incrementalTable.session.compileError(
        new Error(`Warehouse does not support multiple databases`)
      );
      return "";
    }

    return this.incrementalTable.session.finalizeDatabase(
      this.incrementalTable.proto.target.database
    );
  }

  public where(where: Contextable<ITableContext, string>) {
    this.incrementalTable.where(where);
    return "";
  }

  public when(cond: boolean, trueCase: string, falseCase: string = "") {
    return cond ? trueCase : falseCase;
  }

  public incremental() {
    return !!this.isIncremental;
  }

  public preOps(statement: Contextable<ITableContext, string | string[]>) {
    this.incrementalTable.preOps(statement);
    return "";
  }

  public postOps(statement: Contextable<ITableContext, string | string[]>) {
    this.incrementalTable.postOps(statement);
    return "";
  }

  public disabled() {
    this.incrementalTable.disabled();
    return "";
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    this.incrementalTable.bigquery(bigquery);
    return "";
  }

  public dependencies(res: Resolvable) {
    this.incrementalTable.dependencies(res);
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
    this.incrementalTable.tags(tags);
    return "";
  }
}
