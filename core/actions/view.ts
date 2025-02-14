import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import {
  ActionBuilder,
  ILegacyBigQueryOptions,
  LegacyConfigConverter,
  TableType
} from "df/core/actions";
import { Assertion } from "df/core/actions/assertion";
import { IncrementalTable } from "df/core/actions/incremental_table";
import { Table } from "df/core/actions/table";
import { ColumnDescriptors } from "df/core/column_descriptors";
import { Contextable, ITableContext, Resolvable } from "df/core/contextables";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  checkAssertionsForDependency,
  checkExcessProperties,
  configTargetToCompiledGraphTarget,
  nativeRequire,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  strictKeysOf,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 * @deprecated
 * These options are only here to preserve backwards compatibility of legacy config options.
 * TODO(ekrekr): consider breaking backwards compatability of these in v4.
 */
export interface ILegacyViewBigqueryConfig {
  labels: { [key: string]: string };
  additionalOptions: { [key: string]: string };
}

/**
 * Views are virtualised tables. They are useful for creating a new structured table without having
 * to copy the original data to it, which can result in significant cost savings for avoiding data
 * processing and storage.
 *
 * You can create views in the following ways. Available config options are defined in
 * [ViewConfig](configs#dataform-ActionConfig-ViewConfig), and are shared across all the
 * following ways of creating tables.
 *
 * **Using a SQLX file:**
 *
 * ```sql
 * -- definitions/name.sqlx
 * config {
 *   type: "view"
 * }
 * SELECT column FROM someTable
 * ```
 *
 * **Using action configs files:**
 *
 * ```yaml
 * # definitions/actions.yaml
 * actions:
 * - view:
 *   filename: name.sql
 * ```
 *
 * ```sql
 * -- definitions/name.sql
 * SELECT column FROM someTable
 * ```
 *
 * **Using the Javascript API:**
 *
 * ```js
 * // definitions/file.js
 * table("name", { type: "view" }).query("SELECT column FROM someTable")
 * ```
 *
 * Note: When using the Javascript API, methods in this class can be accessed by the returned value.
 * This is where `query` comes from.
 */
export class View extends ActionBuilder<dataform.Table> {
  /**
   * @hidden Stores the generated proto for the compiled graph.
   * <!-- TODO(ekrekr): make this field private, to enforce proto update logic to happen in this
   * class. -->
   */
  public proto = dataform.Table.create({
    type: "view",
    enumType: dataform.TableType.VIEW,
    disabled: false,
    tags: []
  });

  /** @hidden Hold a reference to the Session instance. */
  public session: Session;

  /**
   * @hidden If true, adds the inline assertions of dependencies as direct dependencies for this
   * action.
   */
  public dependOnDependencyAssertions: boolean = false;

  /** @hidden We delay contextification until the final compile step, so hold these here for now. */
  public contextableQuery: Contextable<ITableContext, string>;
  private contextableWhere: Contextable<ITableContext, string>;
  private contextablePreOps: Array<Contextable<ITableContext, string | string[]>> = [];
  private contextablePostOps: Array<Contextable<ITableContext, string | string[]>> = [];

  /** @hidden */
  private uniqueKeyAssertions: Assertion[] = [];
  private rowConditionsAssertion: Assertion;

  /** @hidden */
  private unverifiedConfig: any;
  private configPath: string | undefined;

  /** @hidden */
  constructor(session?: Session, unverifiedConfig?: any, configPath?: string) {
    super(session);
    this.session = session;
    this.configPath = configPath;
    // A copy is used here to prevent manipulation of the original.
    this.unverifiedConfig = Object.assign({}, unverifiedConfig);

    if (!unverifiedConfig) {
      return;
    }

    const config = this.verifyConfig(unverifiedConfig);

    if (!config.name) {
      config.name = Path.basename(config.filename);
    }
    const target = actionConfigToCompiledGraphTarget(config);
    this.proto.target = this.applySessionToTarget(target, session.projectConfig, config.filename, {
      validateTarget: true
    });
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
    if (config.materialized) {
      this.materialized(config.materialized);
    }
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

  /**
   * @deprecated
   * Deprecated in favor of action type can being set in the configs passed to action constructor
   * functions, for example `publish("name", { type: "table" })`.
   */
  public type(type: TableType) {
    let newAction: IncrementalTable | Table;
    switch (type) {
      case "table":
        newAction = new Table(
          this.session,
          { ...this.unverifiedConfig, type: "table" },
          this.configPath
        );
        break;
      case "incremental":
        newAction = new IncrementalTable(
          this.session,
          { ...this.unverifiedConfig, type: "incremental" },
          this.configPath
        );
        break;
      case "view":
        return this;
      default:
        throw new Error(`Unexpected table type: ${type}`);
    }
    const existingAction = this.session.actions.indexOf(this);
    if (existingAction === -1) {
      throw Error(
        "Expected pre-existing action, but none found. Please report this to the Dataform team."
      );
    }
    this.session.actions[existingAction] = newAction;
  }

  /**
   * Sets the query to generate the table from.
   */
  public query(query: Contextable<ITableContext, string>) {
    this.contextableQuery = query;
    return this;
  }

  /** @hidden */
  public where(where: Contextable<ITableContext, string>) {
    this.contextableWhere = where;
    return this;
  }

  /**
   * Sets a pre-operation to run before the query is run. This is often used for temporarily
   * granting permission to access source tables.
   *
   * Example:
   *
   * ```js
   * // definitions/file.js
   * publish("example")
   *   .preOps(ctx => `GRANT \`roles/bigquery.dataViewer\` ON TABLE ${ctx.ref("other_table")} TO "group:automation@example.com"`)
   *   .query(ctx => `SELECT * FROM ${ctx.ref("other_table")}`)
   *   .postOps(ctx => `REVOKE \`roles/bigquery.dataViewer\` ON TABLE ${ctx.ref("other_table")} TO "group:automation@example.com"`)
   * ```
   */
  public preOps(pres: Contextable<ITableContext, string | string[]>) {
    this.contextablePreOps.push(pres);
    return this;
  }

  /**
   * Sets a post-operation to run after the query is run. This is often used for revoking temporary
   * permissions granted to access source tables.
   *
   * Example:
   *
   * ```js
   * // definitions/file.js
   * publish("example")
   *   .preOps(ctx => `GRANT \`roles/bigquery.dataViewer\` ON TABLE ${ctx.ref("other_table")} TO "group:automation@example.com"`)
   *   .query(ctx => `SELECT * FROM ${ctx.ref("other_table")}`)
   *   .postOps(ctx => `REVOKE \`roles/bigquery.dataViewer\` ON TABLE ${ctx.ref("other_table")} TO "group:automation@example.com"`)
   * ```
   */
  public postOps(posts: Contextable<ITableContext, string | string[]>) {
    this.contextablePostOps.push(posts);
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.disabled](configs#dataform-ActionConfig-ViewConfig).
   *
   * If called with `true`, this action is not executed. The action can still be depended upon.
   * Useful for temporarily turning off broken actions.
   */
  public disabled(disabled = true) {
    this.proto.disabled = disabled;
    this.uniqueKeyAssertions.forEach(assertion => assertion.disabled(disabled));
    this.rowConditionsAssertion?.disabled(disabled);
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.materialized](configs#dataform-ActionConfig-ViewConfig).
   *
   * Applies the materialized view optimization, see
   * https://cloud.google.com/bigquery/docs/materialized-views-intro.
   */
  public materialized(materialized: boolean) {
    this.proto.materialized = materialized;
  }

  /**
   * @deprecated Deprecated in favor of options available directly on
   * [ViewConfig](configs#dataform-ActionConfig-ViewConfig).
   *
   * Sets bigquery options for the action.
   */
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

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.dependencies](configs#dataform-ActionConfig-ViewConfig).
   *
   * Sets dependencies of the view.
   */
  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable =>
      this.proto.dependencyTargets.push(checkAssertionsForDependency(this, resolvable))
    );
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.hermetic](configs#dataform-ActionConfig-ViewConfig).
   *
   * If true, this indicates that the action only depends on data from explicitly-declared
   * dependencies. Otherwise if false, it indicates that the  action depends on data from a source
   * which has not been declared as a dependency.
   */
  public hermetic(hermetic: boolean) {
    this.proto.hermeticity = hermetic
      ? dataform.ActionHermeticity.HERMETIC
      : dataform.ActionHermeticity.NON_HERMETIC;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.tags](configs#dataform-ActionConfig-ViewConfig).
   *
   * Sets a list of user-defined tags applied to this action.
   */
  public tags(value: string | string[]) {
    const newTags = typeof value === "string" ? [value] : value;
    newTags.forEach(t => {
      this.proto.tags.push(t);
    });
    this.uniqueKeyAssertions.forEach(assertion => assertion.tags(value));
    this.rowConditionsAssertion?.tags(value);
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.description](configs#dataform-ActionConfig-ViewConfig).
   *
   * Sets the description of this view.
   */
  public description(description: string) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.description = description;
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.columns](configs#dataform-ActionConfig-ViewConfig).
   *
   * Sets the column descriptors of columns in this view.
   */
  public columns(columns: dataform.ActionConfig.ColumnDescriptor[]) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.columns = ColumnDescriptors.mapConfigProtoToCompilationProto(
      columns
    );
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.project](configs#dataform-ActionConfig-ViewConfig).
   *
   * Sets the
   * Sets the database (Google Cloud project ID) in which to create the output of this action.
   */
  public database(database: string) {
    this.proto.target = this.applySessionToTarget(
      dataform.Target.create({ ...this.proto.target, database }),
      this.session.projectConfig,
      this.proto.fileName,
      { validateTarget: true }
    );
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.dataset](configs#dataform-ActionConfig-ViewConfig).
   *
   * Sets the schema (BigQuery dataset) in which to create the output of this action.
   */
  public schema(schema: string) {
    this.proto.target = this.applySessionToTarget(
      dataform.Target.create({ ...this.proto.target, schema }),
      this.session.projectConfig,
      this.proto.fileName,
      { validateTarget: true }
    );
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.assertions](configs#dataform-ActionConfig-ViewConfig).
   *
   * Sets in-line assertions for this view.
   *
   * <!-- Note: this both applies in-line assertions, and acts as a method available via the JS API.
   * Usage of it via the JS API is deprecated, but the way it applies in-line assertions is still
   * needed -->
   */
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

  /**
   * @deprecated Deprecated in favor of
   * [ViewConfig.dependOnDependencyAssertions](configs#dataform-ActionConfig-ViewConfig).
   *
   * When called with `true`, assertions dependent upon any dependency will be add as dedpendency
   * to this action.
   */
  public setDependOnDependencyAssertions(dependOnDependencyAssertions: boolean) {
    this.dependOnDependencyAssertions = dependOnDependencyAssertions;
    return this;
  }

  /** @hidden */
  public getFileName() {
    return this.proto.fileName;
  }

  /** @hidden */
  public getTarget() {
    return dataform.Target.create(this.proto.target);
  }

  /** @hidden */
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

  /** @hidden */
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

  /**
   * @hidden Verify config checks that the constructor provided config matches the expected proto
   * structure, or the previously accepted legacy structure. If the legacy structure is used, it is
   * converted to the new structure.
   */
  private verifyConfig(
    // `any` is used here to facilitate the type merging of the legacy table config, which is very
    // different to the new structure.
    unverifiedConfig: dataform.ActionConfig.ViewConfig | ILegacyBigQueryOptions | any
  ): dataform.ActionConfig.ViewConfig {
    // The "type" field only exists on legacy view configs. Here we convert them to the new format.
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
      if (unverifiedConfig?.bigquery) {
        if (!!unverifiedConfig.bigquery.labels) {
          unverifiedConfig.labels = unverifiedConfig.bigquery.labels;
          delete unverifiedConfig.bigquery.labels;
        }
        if (!!unverifiedConfig.bigquery.additionalOptions) {
          unverifiedConfig.additionalOptions = unverifiedConfig.bigquery.additionalOptions;
          delete unverifiedConfig.bigquery.additionalOptions;
        }
        checkExcessProperties(
          (e: Error) => {
            throw e;
          },
          unverifiedConfig.bigquery,
          strictKeysOf<ILegacyViewBigqueryConfig>()(["labels", "additionalOptions"]),
          "BigQuery view config"
        );
      }
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
  constructor(private view: View, private isIncremental = false) {}

  public self(): string {
    return this.resolve(this.view.proto.target);
  }

  public name(): string {
    return this.view.session.finalizeName(this.view.proto.target.name);
  }

  public ref(ref: Resolvable | string[], ...rest: string[]): string {
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      this.view.session.compileError(new Error(`Action name is not specified`));
      return "";
    }
    this.view.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.view.session.resolve(ref, ...rest);
  }

  public schema(): string {
    return this.view.session.finalizeSchema(this.view.proto.target.schema);
  }

  public database(): string {
    if (!this.view.proto.target.database) {
      this.view.session.compileError(new Error(`Warehouse does not support multiple databases`));
      return "";
    }

    return this.view.session.finalizeDatabase(this.view.proto.target.database);
  }

  public where(where: Contextable<ITableContext, string>) {
    this.view.where(where);
    return "";
  }

  public when(cond: boolean, trueCase: string, falseCase: string = "") {
    return cond ? trueCase : falseCase;
  }

  public incremental() {
    return !!this.isIncremental;
  }

  public preOps(statement: Contextable<ITableContext, string | string[]>) {
    this.view.preOps(statement);
    return "";
  }

  public postOps(statement: Contextable<ITableContext, string | string[]>) {
    this.view.postOps(statement);
    return "";
  }

  public disabled() {
    this.view.disabled();
    return "";
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    this.view.bigquery(bigquery);
    return "";
  }

  public dependencies(res: Resolvable) {
    this.view.dependencies(res);
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
    this.view.tags(tags);
    return "";
  }
}
