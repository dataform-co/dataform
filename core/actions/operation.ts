import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { ColumnDescriptors } from "df/core/column_descriptors";
import { Contextable, IActionContext, Resolvable } from "df/core/contextables";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  checkAssertionsForDependency,
  configTargetToCompiledGraphTarget,
  nativeRequire,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  toResolvable
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 * @deprecated
 * This maintains backwards compatability with older versions.
 * TODO(ekrekr): consider breaking backwards compatability of these in v4.
 */
interface ILegacyOperationConfig extends dataform.ActionConfig.OperationConfig {
  dependencies: Resolvable[];
  database: string;
  schema: string;
  fileName: string;
  type: string;
}

/**
 * Operations define custom SQL operations that don't fit into the Dataform model of publishing a
 * table or writing an assertion.
 *
 * You can create operations in the following ways. Available config options are defined in
 * [OperationConfig](configs#dataform-ActionConfig-OperationConfig), and are shared across all the
 * following ways of creating operations.
 *
 * **Using a SQLX file:**
 *
 * ```sql
 * -- definitions/name.sqlx
 * config {
 *   type: "operations"
 * }
 * DELETE FROM dataset.table WHERE country = 'GB'
 * ```
 *
 * **Using action configs files:**
 *
 * ```yaml
 * # definitions/actions.yaml
 * actions:
 * - operation:
 *   filename: name.sql
 * ```
 *
 * ```sql
 * -- definitions/name.sql
 * DELETE FROM dataset.table WHERE country = 'GB'
 * ```
 *
 * **Using the Javascript API:**
 *
 * ```js
 * // definitions/file.js
 * operate("name").query("DELETE FROM dataset.table WHERE country = 'GB'")
 * ```
 *
 * Note: When using the Javascript API, methods in this class can be accessed by the returned value.
 * This is where `query` comes from.
 */
export class Operation extends ActionBuilder<dataform.Operation> {
  /** @hidden Hold a reference to the Session instance. */
  public session: Session;

  /**
   * @hidden If true, adds the inline assertions of dependencies as direct dependencies for this
   * action.
   */
  public dependOnDependencyAssertions: boolean = false;

  /**
   * @hidden Stores the generated proto for the compiled graph.
   */
  private proto = dataform.Operation.create();

  /** @hidden We delay contextification until the final compile step, so hold these here for now. */
  private contextableQueries: Contextable<IActionContext, string | string[]>;

  /** @hidden */
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
    this.proto.target = this.applySessionToTarget(target, session.projectConfig, config.filename, {
      validateTarget: true
    });
    this.proto.canonicalTarget = this.applySessionToTarget(target, session.canonicalProjectConfig);

    if (configPath) {
      config.filename = resolveActionsConfigFilename(config.filename, configPath);
      this.queries(nativeRequire(config.filename).query);
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
    if (config.hasOutput) {
      this.hasOutput(config.hasOutput);
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
    if (config.filename) {
      this.proto.fileName = config.filename;
    }
    return this;
  }

  /**
   * Sets the query/queries to generate the operation from.
   *
   * <!-- TODO(ekrekr): deprecated this in favor of a single `query(` method -->
   */
  public queries(queries: Contextable<IActionContext, string | string[]>) {
    this.contextableQueries = queries;
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [OperationConfig.dependencies](configs#dataform-ActionConfig-OperationConfig).
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
   * [OperationConfig.hermetic](configs#dataform-ActionConfig-OperationConfig).
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
   * [OperationConfig.disabled](configs#dataform-ActionConfig-OperationConfig).
   *
   * If called with `true`, this action is not executed. The action can still be depended upon.
   * Useful for temporarily turning off broken actions.
   */
  public disabled(disabled = true) {
    this.proto.disabled = disabled;
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [OperationConfig.tags](configs#dataform-ActionConfig-OperationConfig).
   *
   * Sets a list of user-defined tags applied to this action.
   */
  public tags(value: string | string[]) {
    const newTags = typeof value === "string" ? [value] : value;
    newTags.forEach(t => {
      if (this.proto.tags.indexOf(t) < 0) {
        this.proto.tags.push(t);
      }
    });
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [OperationConfig.hasOutput](configs#dataform-ActionConfig-OperationConfig).
   *
   * Declares that this action creates a dataset which should be referenceable as a dependency
   * target, for example by using the `ref` function.
   */
  public hasOutput(hasOutput: boolean) {
    this.proto.hasOutput = hasOutput;
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [OperationConfig.description](configs#dataform-ActionConfig-OperationConfig).
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
   * [OperationConfig.columns](configs#dataform-ActionConfig-OperationConfig).
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
   * [OperationConfig.project](configs#dataform-ActionConfig-OperationConfig).
   *
   * Sets the database (Google Cloud project ID) in which to create the corresponding view for this
   * operation.
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
   * [OperationConfig.dataset](configs#dataform-ActionConfig-OperationConfig).
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
   * [OperationConfig.dependOnDependencyAssertions](configs#dataform-ActionConfig-OperationConfig).
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
  public getHasOutput() {
    return this.proto.hasOutput;
  }

  /** @hidden */
  public setFilename() {
    return this.proto.hasOutput;
  }

  /** @hidden */
  public compile() {
    if (this.proto.actionDescriptor?.columns?.length > 0 && !this.proto.hasOutput) {
      this.session.compileError(
        new Error(
          "Actions of type 'operations' may only describe columns if they specify 'hasOutput: true'."
        ),
        this.proto.fileName
      );
    }

    const context = new OperationContext(this);

    const appliedQueries = context.apply(this.contextableQueries);
    this.proto.queries = typeof appliedQueries === "string" ? [appliedQueries] : appliedQueries;

    return verifyObjectMatchesProto(
      dataform.Operation,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
  }

  /**
   * @hidden Verify config checks that the constructor provided config matches the expected proto
   * structure, or the previously accepted legacy structure. If the legacy structure is used, it is
   * converted to the new structure.
   */
  private verifyConfig(
    unverifiedConfig: ILegacyOperationConfig
  ): dataform.ActionConfig.OperationConfig {
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
    }
    return verifyObjectMatchesProto(
      dataform.ActionConfig.OperationConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    );
  }
}

/**
 * @hidden
 */
export class OperationContext implements IActionContext {
  private operation?: Operation;

  constructor(operation: Operation) {
    this.operation = operation;
  }

  public self(): string {
    return this.resolve(this.operation.getTarget());
  }

  public name(): string {
    return this.operation.session.finalizeName(this.operation.getTarget().name);
  }

  public ref(ref: Resolvable | string[], ...rest: string[]) {
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      this.operation.session.compileError(new Error(`Action name is not specified`));
      return "";
    }
    this.operation.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.operation.session.resolve(ref, ...rest);
  }

  public schema(): string {
    return this.operation.session.finalizeSchema(this.operation.getTarget().schema);
  }

  public database(): string {
    if (!this.operation.getTarget().database) {
      this.operation.session.compileError(
        new Error(`Warehouse does not support multiple databases`)
      );
      return "";
    }

    return this.operation.session.finalizeDatabase(this.operation.getTarget().database);
  }

  public dependencies(name: Resolvable | Resolvable[]) {
    this.operation.dependencies(name);
    return "";
  }

  public tags(name: string | string[]) {
    this.operation.tags(name);
    return "";
  }

  public hasOutput(hasOutput: boolean) {
    this.operation.hasOutput(hasOutput);
    return "";
  }

  public when(cond: boolean, trueCase: string, falseCase: string = "") {
    return cond ? trueCase : falseCase;
  }

  public apply<T>(value: Contextable<IActionContext, T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
