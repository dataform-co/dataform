import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import {
  ActionBuilder,
  checkConfigAdditionalOptionsOverlap,
  ILegacyBigQueryOptions,
  ILegacyTableConfig,
  LegacyConfigConverter,
  TableType
} from "df/core/actions";
import { Assertion } from "df/core/actions/assertion";
import { IncrementalTable } from "df/core/actions/incremental_table";
import { View } from "df/core/actions/view";
import { ColumnDescriptors } from "df/core/column_descriptors";
import {
  Contextable,
  ITableContext,
  Resolvable,
} from "df/core/contextables";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  checkAssertionsForDependency,
  checkExcessProperties,
  getConnectionForIcebergTable,
  getEffectiveTableFolderSubpath,
  getFileFormatValueForIcebergTable,
  getStorageUriForIcebergTable,
  nativeRequire,
  resolvableAsActionConfigTarget,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  strictKeysOf,
  toResolvable,
  validateConnectionFormat,
  validateQueryString,
  validateStorageUriFormat,
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * Tables are the fundamental building block for storing data when using Dataform. Dataform compiles
 * your Dataform core code into SQL, executes the SQL code, and creates your defined tables in
 * BigQuery.
 *
 * You can create tables in the following ways. Available config options are defined in
 * [TableConfig](configs#dataform-ActionConfig-TableConfig), and are shared across all the
 * following ways of creating tables.
 *
 * **Using a SQLX file:**
 *
 * ```sql
 * -- definitions/name.sqlx
 * config {
 *   type: "table"
 * }
 * SELECT 1
 * ```
 *
 * **Using action configs files:**
 *
 * ```yaml
 * # definitions/actions.yaml
 * actions:
 * - table:
 *   filename: name.sql
 * ```
 *
 * ```sql
 * -- definitions/name.sql
 * SELECT 1
 * ```
 *
 * **Using the Javascript API:**
 *
 * ```js
 * // definitions/file.js
 * table("name", { type: "table" }).query("SELECT 1 AS TEST")
 * ```
 *
 * Note: When using the Javascript API, methods in this class can be accessed by the returned value.
 * This is where `query` comes from.
 */
export class Table extends ActionBuilder<dataform.Table> {
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

  /**
   * @hidden Stores the generated proto for the compiled graph.
   */
  private proto = dataform.Table.create({
    type: "table",
    enumType: dataform.TableType.TABLE,
    disabled: false,
    tags: []
  });

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
    if (config.filename) {
      this.proto.fileName = config.filename;
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
    if (config.assertions) {
      this.assertions(dataform.ActionConfig.TableAssertionsConfig.create(config.assertions));
    }
    if (config.preOperations) {
      this.preOps(config.preOperations);
    }
    if (config.postOperations) {
      this.postOps(config.postOperations);
    }
    this.bigquery({
      partitionBy: config.partitionBy,
      clusterBy: config.clusterBy,
      labels: config.labels,
      partitionExpirationDays: config.partitionExpirationDays,
      requirePartitionFilter: config.requirePartitionFilter,
      additionalOptions: config.additionalOptions,
      ...(config.iceberg ? {
        connection: getConnectionForIcebergTable(config.iceberg.connection),
        fileFormat: getFileFormatValueForIcebergTable(config.iceberg.fileFormat?.toString()),
        tableFormat: dataform.TableFormat.ICEBERG,
        storageUri: getStorageUriForIcebergTable(
          config.iceberg.bucketName,
          getEffectiveTableFolderSubpath(this.proto.target.schema, this.proto.target.name, config.iceberg.tableFolderSubpath),
          config.iceberg.tableFolderRoot,
        ),
      } : {}),
    });

    return this;
  }

  /**
   * @deprecated
   * Deprecated in favor of action type can being set in the configs passed to action constructor
   * functions, for example `publish("name", { type: "table" })`.
   */
  public type(type: TableType) {
    let newAction: View | IncrementalTable;
    switch (type) {
      case "table":
        return this;
      case "incremental":
        newAction = new IncrementalTable(
          this.session,
          { ...this.unverifiedConfig, type: "incremental" },
          this.configPath
        );
        break;
      case "view":
        newAction = new View(
          this.session,
          { ...this.unverifiedConfig, type: "view" },
          this.configPath
        );
        break;
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
   * [TableConfig.disabled](configs#dataform-ActionConfig-TableConfig).
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
   * @deprecated Deprecated in favor of options available directly on
   * [TableConfig](configs#dataform-ActionConfig-TableConfig). For example:
   * `publish("name", { type: "table", partitionBy: "column" }`).
   *
   * Sets bigquery options for the action.
   */
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

  /**
   * @deprecated Deprecated in favor of
   * [TableConfig.dependencies](configs#dataform-ActionConfig-TableConfig).
   *
   * Sets dependencies of the table.
   */
  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable => {
      const dependencyTarget = checkAssertionsForDependency(this, resolvable);
      if (!!dependencyTarget) {
        this.proto.dependencyTargets.push(dependencyTarget);
      }
    });
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [TableConfig.hermetic](configs#dataform-ActionConfig-TableConfig).
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
   * [TableConfig.tags](configs#dataform-ActionConfig-TableConfig).
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
   * [TableConfig.description](configs#dataform-ActionConfig-TableConfig).
   *
   * Sets the description of this assertion.
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
   * [TableConfig.columns](configs#dataform-ActionConfig-TableConfig).
   *
   * Sets the column descriptors of columns in this table.
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
   * [TableConfig.project](configs#dataform-ActionConfig-TableConfig).
   *
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
   * [TableConfig.dataset](configs#dataform-ActionConfig-TableConfig).
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
   * [TableConfig.assertions](configs#dataform-ActionConfig-TableConfig).
   *
   * Sets in-line assertions for this table.
   *
   * <!-- Note: this both applies in-line assertions, and acts as a method available via the JS API.
   * Usage of it via the JS API is deprecated, but the way it applies in-line assertions is still
   * needed -->
   */
  public assertions(tableAssertionsConfig: dataform.ActionConfig.TableAssertionsConfig): Table {
    const inlineAssertions = this.generateInlineAssertions(tableAssertionsConfig, this.proto);
    this.uniqueKeyAssertions = inlineAssertions.uniqueKeyAssertions;
    this.rowConditionsAssertion = inlineAssertions.rowConditionsAssertion;
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [TableConfig.dependOnDependencyAssertions](configs#dataform-ActionConfig-TableConfig).
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

    if (this.proto.bigquery?.connection) {
      validateConnectionFormat(this.proto.bigquery.connection);
    }

    if (this.proto.bigquery?.storageUri) {
      validateStorageUriFormat(this.proto.bigquery.storageUri);
    }

    return verifyObjectMatchesProto(
      dataform.Table,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
  }

  /** @hidden */
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

  /**
   * @hidden Verify config checks that the constructor provided config matches the expected proto
   * structure, or the previously accepted legacy structure. If the legacy structure is used, it is
   * converted to the new structure.
   */
  private verifyConfig(
    // `any` is used here to facilitate the type merging of the legacy table config, which is very
    // different to the new structure.
    unverifiedConfig: dataform.ActionConfig.TableConfig | ILegacyTableConfig | any
  ): dataform.ActionConfig.TableConfig {
    // The "type" field only exists on legacy table configs. Here we convert them to the
    // new format.
    if (unverifiedConfig.type) {
      if (unverifiedConfig.type !== "table") {
        throw ReferenceError(
          `Unexpected type for Table; want "table", got ${unverifiedConfig.type}`
        );
      }
      delete unverifiedConfig.type;
      if (unverifiedConfig.dependencies) {
        unverifiedConfig.dependencyTargets = unverifiedConfig.dependencies.map(
          (dependency: string | object) => resolvableAsActionConfigTarget(dependency)
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
      if (unverifiedConfig.iceberg) {
        if (
          unverifiedConfig.iceberg.fileFormat &&
          unverifiedConfig.iceberg.fileFormat.toUpperCase() !== 'PARQUET'
        ) {
          throw new ReferenceError(
            `Unexpected file format; only "PARQUET" is allowed, got "${unverifiedConfig.iceberg.fileFormat}".`
          );
        }
        if (!unverifiedConfig.iceberg.bucketName) {
          throw new ReferenceError(
            'Reference error: bucket_name must be defined in an iceberg subblock.'
          );
        }
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
          strictKeysOf<ILegacyBigQueryOptions>()([
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
    }

    const config = verifyObjectMatchesProto(
      dataform.ActionConfig.TableConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    );

    if (!config.partitionBy && (config.partitionExpirationDays || config.requirePartitionFilter)) {
      this.session.compileError(
        `requirePartitionFilter/partitionExpirationDays are not valid for non partitioned BigQuery tables`,
        config.filename,
        dataform.Target.create({
          database: config.project,
          schema: config.dataset,
          name: config.name
        })
      );
    }

    if (config.additionalOptions) {
      checkConfigAdditionalOptionsOverlap(config, this.session);
    }

    return config;
  }
}

/**
 * @hidden
 */
export class TableContext implements ITableContext {
  constructor(private table: Table, private isIncremental = false) {}

  public self(): string {
    return this.resolve(this.table.getTarget());
  }

  public name(): string {
    return this.table.session.finalizeName(this.table.getTarget().name);
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
    return this.table.session.finalizeSchema(this.table.getTarget().schema);
  }

  public database(): string {
    if (!this.table.getTarget().database) {
      this.table.session.compileError(new Error(`Warehouse does not support multiple databases`));
      return "";
    }

    return this.table.session.finalizeDatabase(this.table.getTarget().database);
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
