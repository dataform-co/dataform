import {
  IActionConfig,
  ICommonContext,
  IDependenciesConfig,
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
 * Configuration options for `assertion` action types.
 */
export interface IAssertionConfig
  extends IActionConfig,
    IDependenciesConfig,
    INamedConfig,
    ITargetableConfig {
  /**
   * The database where the corresponding view for this assertion should be created.
   */
  database?: string;

  /**
   * The schema where the corresponding view for this assertion should be created.
   */
  schema?: string;

  /**
   * A description for this assertion.
   */
  description?: string;
}

export const IAssertionConfigProperties = strictKeysOf<IAssertionConfig>()([
  "database",
  "dependencies",
  "description",
  "disabled",
  "hermetic",
  "name",
  "schema",
  "tags",
  "type"
]);

/**
 * @hidden
 */
export type AContextable<T> = T | ((ctx: AssertionContext) => T);

/**
 * @hidden
 */
export class Assertion {
  public proto: dataform.IAssertion = dataform.Assertion.create();

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: AContextable<string>;

  public config(config: IAssertionConfig) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      config,
      IAssertionConfigProperties,
      "assertion config"
    );
    if (config.dependencies) {
      this.dependencies(config.dependencies);
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
    if (config.database) {
      this.database(config.database);
    }
    if (config.schema) {
      this.schema(config.schema);
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
      this.proto.dependencyTargets.push(resolvableAsTarget(resolvable));
    });
    return this;
  }

  public hermetic(hermetic: boolean) {
    this.proto.hermeticity = hermetic
      ? dataform.ActionHermeticity.HERMETIC
      : dataform.ActionHermeticity.NON_HERMETIC;
  }

  public disabled() {
    this.proto.disabled = true;
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

  public compile() {
    const context = new AssertionContext(this);

    this.proto.query = context.apply(this.contextableQuery);
    validateQueryString(this.session, this.proto.query, this.proto.fileName);

    return this.proto;
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
    return this.assertion.proto.target.name;
  }

  public ref(ref: Resolvable | string[], ...rest: string[]) {
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      const message = `Action name is not specified`;
      this.assertion.session.compileError(new Error(message));
      return "";
    }
    this.assertion.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.assertion.session.resolve(toResolvable(ref, rest));
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
