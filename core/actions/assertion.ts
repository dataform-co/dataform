import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { ICommonContext, Resolvable } from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  nativeRequire,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  setNameAndTarget,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 */
export type AContextable<T> = T | ((ctx: AssertionContext) => T);

/**
 * @hidden
 */
export class Assertion extends ActionBuilder<dataform.Assertion> {
  // TODO(ekrekr): make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.IAssertion = dataform.Assertion.create();

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: AContextable<string>;

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
      true,
      true
    );
    this.proto.canonicalTarget = this.applySessionToTarget(
      target,
      session.canonicalProjectConfig,
      undefined,
      false,
      true
    );

    if (configPath) {
      config.filename = resolveActionsConfigFilename(config.filename, configPath);
      this.query(nativeRequire(config.filename).query);
    }

    // TODO(ekrekr): load config proto column descriptors.

    if (config.dependencyTargets) {
      this.dependencies(
        config.dependencyTargets.map(dependencyTarget =>
          actionConfigToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
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

  public query(query: AContextable<string>) {
    this.contextableQuery = query;
    return this;
  }

  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable => {
      const resolvableTarget = resolvableAsTarget(resolvable);
      this.session.actionAssertionMap.set(resolvableTarget, this);
      this.proto.dependencyTargets.push(resolvableTarget);
    });
    return this;
  }

  public hermetic(hermetic: boolean) {
    this.proto.hermeticity = hermetic
      ? dataform.ActionHermeticity.HERMETIC
      : dataform.ActionHermeticity.NON_HERMETIC;
  }

  public disabled(disabled = true) {
    this.proto.disabled = disabled;
    return this;
  }

  public tags(value: string | string[]) {
    const newTags = typeof value === "string" ? [value] : value;
    newTags.forEach(t => {
      if (this.proto.tags.indexOf(t) < 0) {
        this.proto.tags.push(t);
      }
    });
    return this;
  }

  public description(description: string) {
    this.proto.actionDescriptor = { description };
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
    const context = new AssertionContext(this);

    this.proto.query = context.apply(this.contextableQuery);
    validateQueryString(this.session, this.proto.query, this.proto.fileName);

    return verifyObjectMatchesProto(
      dataform.Assertion,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
  }

  private verifyConfig(unverifiedConfig: any): dataform.ActionConfig.AssertionConfig {
    // This maintains backwards compatability with older versions.
    // TODO(ekrekr): break backwards compatability of these in v4.
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

    // TODO(ekrekr): move this to a shared location after all action builders have proto config
    // verifiers.
    if (unverifiedConfig.type) {
      delete unverifiedConfig.type;
    }

    console.log("🚀 ~ Assertion ~ verifyConfig ~ unverifiedConfig:", unverifiedConfig);
    return verifyObjectMatchesProto(dataform.ActionConfig.AssertionConfig, unverifiedConfig);
  }
}

/**
 * @hidden
 */
export class AssertionContext implements ICommonContext {
  private assertion?: Assertion;

  constructor(assertion: Assertion) {
    this.assertion = assertion;
  }

  public self(): string {
    return this.resolve(this.assertion.proto.target);
  }

  public name(): string {
    return this.assertion.session.finalizeName(this.assertion.proto.target.name);
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
    return this.assertion.session.finalizeSchema(this.assertion.proto.target.schema);
  }

  public database(): string {
    if (!this.assertion.proto.target.database) {
      this.assertion.session.compileError(
        new Error(`Warehouse does not support multiple databases`)
      );
      return "";
    }

    return this.assertion.session.finalizeDatabase(this.assertion.proto.target.database);
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
