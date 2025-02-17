import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { IActionContext, Resolvable } from "df/core/contextables";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  configTargetToCompiledGraphTarget,
  nativeRequire,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 * @deprecated
 * This maintains backwards compatability with older versions.
 * TODO(ekrekr): consider breaking backwards compatability of these in v4.
 */
interface ILegacyAssertionConfig extends dataform.ActionConfig.AssertionConfig {
  dependencies: Resolvable[];
  database: string;
  schema: string;
  fileName: string;
  type: string;
}

/** @hidden */
export type AContextable<T> = T | ((ctx: AssertionContext) => T);

/**
 * An assertion is a data quality test query that finds rows that violate one or more conditions
 * specified in the query. If the query returns any rows, the assertion fails.
 *
 * You can create assertions in the following ways. Available config options are defined in
 * [AssertionConfig](configs#dataform-ActionConfig-AssertionConfig), and are shared across all the
 * following ways of creating assertions.
 *
 * **Using a SQLX file:**
 *
 * ```sql
 * -- definitions/name.sqlx
 * config {
 *   type: "assertion"
 * }
 * SELECT * FROM table WHERE a IS NULL
 * ```
 *
 * **Using built-in assertions in the config block of a table:**
 *
 * See [TableConfig.assertions](configs#dataform-ActionConfig-TableConfig)
 *
 * **Using action configs files:**
 *
 * ```yaml
 * # definitions/actions.yaml
 * actions:
 * - assertion:
 *   filename: name.sql
 * ```
 *
 * ```sql
 * -- definitions/name.sql
 * SELECT * FROM table WHERE a IS NULL
 * ```
 *
 * **Using the Javascript API:**
 *
 * ```js
 * // definitions/file.js
 * assert("name").query("SELECT * FROM table WHERE a IS NULL")
 * ```
 *
 * Note: When using the Javascript API, methods in this class can be accessed by the returned value.
 * This is where `query` comes from.
 */
export class Assertion extends ActionBuilder<dataform.Assertion> {
  /**
   * @hidden Stores the generated proto for the compiled graph.
   */
  private proto = dataform.Assertion.create();

  /** @hidden Hold a reference to the Session instance. */
  public session: Session;

  /** @hidden We delay contextification until the final compile step, so hold these here for now. */
  private contextableQuery: AContextable<string>;

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
      validateTarget: true,
      useDefaultAssertionDataset: true
    });
    this.proto.canonicalTarget = this.applySessionToTarget(
      target,
      session.canonicalProjectConfig,
      undefined,
      { validateTarget: false, useDefaultAssertionDataset: true }
    );

    if (configPath) {
      config.filename = resolveActionsConfigFilename(config.filename, configPath);
      this.query(nativeRequire(config.filename).query);
    }

    if (config.dependencyTargets) {
      this.dependencies(
        config.dependencyTargets.map(dependencyTarget =>
          configTargetToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
        )
      );
    }
    if (config.hermetic) {
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
   * Sets the query to be run by the assertion.
   */
  public query(query: AContextable<string>) {
    this.contextableQuery = query;
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [AssertionConfig.dependencies](configs#dataform-ActionConfig-AssertionConfig).
   *
   * Sets dependencies of the assertion.
   */
  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable => {
      const resolvableTarget = resolvableAsTarget(resolvable);
      this.session.actionAssertionMap.set(resolvableTarget, this);
      this.proto.dependencyTargets.push(resolvableTarget);
    });
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [AssertionConfig.hermetic](configs#dataform-ActionConfig-AssertionConfig).
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
   * [AssertionConfig.disabled](configs#dataform-ActionConfig-AssertionConfig).
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
   * [AssertionConfig.tags](configs#dataform-ActionConfig-AssertionConfig).
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
   * [AssertionConfig.description](configs#dataform-ActionConfig-AssertionConfig).
   *
   * Sets the description of this assertion.
   */
  public description(description: string) {
    this.proto.actionDescriptor = { description };
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [AssertionConfig.project](configs#dataform-ActionConfig-AssertionConfig).
   *
   * Sets the database (Google Cloud project ID) in which to create the corresponding view for this
   * assertion.
   */
  public database(database: string) {
    this.proto.target = this.applySessionToTarget(
      dataform.Target.create({ ...this.proto.target, database }),
      this.session.projectConfig,
      this.proto.fileName,
      { validateTarget: true, useDefaultAssertionDataset: true }
    );
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [AssertionConfig.dataset](configs#dataform-ActionConfig-AssertionConfig).
   *
   * Sets the schema (BigQuery dataset) in which to create the corresponding view for this
   * assertion.
   */
  public schema(schema: string) {
    this.proto.target = this.applySessionToTarget(
      dataform.Target.create({ ...this.proto.target, schema }),
      this.session.projectConfig,
      this.proto.fileName,
      { validateTarget: true, useDefaultAssertionDataset: true }
    );
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
  public getParentAction() {
    return dataform.Target.create(this.proto.parentAction);
  }

  /** @hidden */
  public setParentAction(target: dataform.Target) {
    return (this.proto.parentAction = target);
  }

  /** @hidden */
  public setFilename(filename: string) {
    return (this.proto.fileName = filename);
  }

  /** @hidden */
  public compile() {
    const context = new AssertionContext(this);

    this.proto.query = context.apply(this.contextableQuery);
    validateQueryString(this.session, this.proto.query, this.proto.fileName);

    return verifyObjectMatchesProto(
      dataform.Assertion,
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
    unverifiedConfig: ILegacyAssertionConfig
  ): dataform.ActionConfig.AssertionConfig {
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

    // TODO(ekrekr): consider moving this to a shared location after all action builders have proto
    // config verifiers.
    if (unverifiedConfig.type) {
      delete unverifiedConfig.type;
    }

    return verifyObjectMatchesProto(
      dataform.ActionConfig.AssertionConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    );
  }
}

/**
 * @hidden
 */
export class AssertionContext implements IActionContext {
  private assertion?: Assertion;

  constructor(assertion: Assertion) {
    this.assertion = assertion;
  }

  public self(): string {
    return this.resolve(this.assertion.getTarget());
  }

  public name(): string {
    return this.assertion.session.finalizeName(this.assertion.getTarget().name);
  }

  public ref(ref: Resolvable | string[], ...rest: string[]) {
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      this.assertion.session.compileError(new Error(`Action name is not specified`));
      return "";
    }
    this.assertion.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.assertion.session.resolve(ref, ...rest);
  }

  public schema(): string {
    return this.assertion.session.finalizeSchema(this.assertion.getTarget().schema);
  }

  public database(): string {
    if (!this.assertion.getTarget().database) {
      this.assertion.session.compileError(
        new Error(`Warehouse does not support multiple databases`)
      );
      return "";
    }

    return this.assertion.session.finalizeDatabase(this.assertion.getTarget().database);
  }

  public dependencies(name: Resolvable | Resolvable[]) {
    this.assertion.dependencies(name);
    return "";
  }

  public tags(name: string | string[]) {
    this.assertion.tags(name);
    return "";
  }

  public when(cond: boolean, trueCase: string, falseCase: string = "") {
    return cond ? trueCase : falseCase;
  }

  public apply<T>(value: AContextable<T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
