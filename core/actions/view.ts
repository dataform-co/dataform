import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { ColumnDescriptors, LegacyColumnDescriptors } from "df/core/column_descriptors";
import { Resolvable, Contextable } from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  addDependenciesToActionDependencyTargets,
  nativeRequire,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  setNameAndTarget,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";
import { Assertion } from "./assertion";
import { ITableContext, Table, TableType } from "./table";

/**
 * @hidden
 * This maintains backwards compatability with older versions.
 * TODO(ekrekr): consider breaking backwards compatability of these in v4.
 */
interface ILegacyViewConfig extends dataform.ActionConfig.ViewConfig {
  dependencies: Resolvable[];
  database: string;
  schema: string;
  fileName: string;
  type: string;
  bigquery: {
    labels: { [key: string]: string };
    additionalOptions: { [key: string]: string };
  };
  // Legacy view config's table assertions cannot directly extend the protobuf view config
  // definition because of legacy view config's flexible types.
  assertions: any;
}

/**
 * @hidden
 */
export class View extends ActionBuilder<dataform.Table> {
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

  constructor(session?: Session, unverifiedConfig?: any, configPath?: string) {
    super(session);
    this.session = session;

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
      this.dependencies(config.dependencyTargets);
    }
    if (config.hermetic !== undefined) {
      this.hermetic(config.hermetic);
    }
    if (config.disabled) {
      this.disabled();
    }
    if (Object.keys(config.additionalOptions).length > 0) {
      this.bigquery({ additionalOptions: config.additionalOptions });
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
    if (config.materialized) {
      this.materialized(config.materialized);
    }
    this.query(nativeRequire(config.filename).query);
    if (config.preOperations) {
      this.preOps(config.preOperations);
    }
    if (config.postOperations) {
      this.postOps(config.postOperations);
    }
    if (Object.keys(config.labels).length || Object.keys(config.additionalOptions).length) {
      this.bigquery({ labels: config.labels, additionalOptions: config.additionalOptions });
    }
    if (config.filename) {
      this.proto.fileName = config.filename;
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
          uniqueKey: ["TableAssertionsConfig"]
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
    const context = new ViewContext(this);
    const incrementalContext = new ViewContext(this, true);

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
    currentContext: ViewContext
  ) {
    let protoOps: string[] = [];
    contextableOps.forEach(contextableOp => {
      const appliedOps = currentContext.apply(contextableOp);
      protoOps = protoOps.concat(typeof appliedOps === "string" ? [appliedOps] : appliedOps);
    });
    return protoOps;
  }

  private verifyConfig(unverifiedConfig: ILegacyViewConfig): dataform.ActionConfig.ViewConfig {
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
      // TODO(ekrekr) columns in their current config format are a difficult structure to represent
      // as protos. They are nested, and use the object keys as the names. Consider a forced
      // migration to the proto style column names.
      unverifiedConfig.columns = ColumnDescriptors.mapLegacyObjectToConfigProto(
        unverifiedConfig.columns as any
      );
    }
    if (unverifiedConfig?.assertions) {
      if (unverifiedConfig.assertions.uniqueKey) {
        unverifiedConfig.assertions.uniqueKey = unverifiedConfig.assertions.uniqueKey;
      }
      // This determines if the uniqueKeys is of the legacy type.
      if (unverifiedConfig.assertions.uniqueKeys?.[0]?.length > 0) {
        unverifiedConfig.assertions.uniqueKeys = (unverifiedConfig.assertions
          .uniqueKeys as string[][]).map(uniqueKey =>
          dataform.ActionConfig.TableAssertionsConfig.UniqueKey.create({ uniqueKey })
        );
      }
      if (typeof unverifiedConfig.assertions.nonNull === "string") {
        unverifiedConfig.assertions.nonNull = [unverifiedConfig.assertions.nonNull];
      }
    }
    if (unverifiedConfig?.bigquery) {
      if (!!unverifiedConfig.bigquery.labels) {
        unverifiedConfig.labels = unverifiedConfig.bigquery.labels;
      }
      if (!!unverifiedConfig.bigquery.additionalOptions) {
        unverifiedConfig.additionalOptions = unverifiedConfig.bigquery.additionalOptions;
      }
      delete unverifiedConfig.bigquery;
    }
    if (unverifiedConfig.type) {
      delete unverifiedConfig.type;
    }

    return verifyObjectMatchesProto(
      dataform.ActionConfig.ViewConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    );
  }
}

/**
 * @hidden
 */
export class ViewContext implements ITableContext {
  constructor(private table: Table | View, private isIncremental = false) {}

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
