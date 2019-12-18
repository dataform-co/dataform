import { Resolvable, Session } from "@dataform/core/session";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

export type AContextable<T> = T | ((ctx: AssertionContext) => T);

export interface AConfig {
  dependencies?: Resolvable | Resolvable[];
  tags?: string[];
  description?: string;
  database?: string;
  schema?: string;
}

export class Assertion {
  public proto: dataform.IAssertion = dataform.Assertion.create();

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: AContextable<string>;

  public config(config: AConfig) {
    if (config.dependencies) {
      this.dependencies(config.dependencies);
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
    const newDependencies = utils.isResolvable(value) ? [value] : value;
    newDependencies.forEach(resolvable => {
      this.proto.dependencyTargets.push(utils.resolvableAsTarget(resolvable));
    });
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
    this.session.setNameAndTarget(
      this.proto,
      this.proto.target.name,
      this.proto.target.schema,
      database
    );
    return this;
  }

  public schema(schema: string) {
    this.session.setNameAndTarget(this.proto, this.proto.target.name, schema);
    return this;
  }

  public compile() {
    const context = new AssertionContext(this);

    const appliedQuery = context.apply(this.contextableQuery);
    this.proto.query = appliedQuery;

    return this.proto;
  }
}

export class AssertionContext {
  private assertion?: Assertion;

  constructor(assertion: Assertion) {
    this.assertion = assertion;
  }

  public ref(ref: Resolvable) {
    if (!utils.resolvableAsTarget(ref)) {
      const message = `Action name is not specified`;
      this.assertion.session.compileError(new Error(message));
      return "";
    }
    this.assertion.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable) {
    return this.assertion.session.resolve(ref);
  }

  public dependencies(name: Resolvable | Resolvable[]) {
    this.assertion.dependencies(name);
    return "";
  }
  public tags(name: string | string[]) {
    this.assertion.tags(name);
    return "";
  }

  public apply<T>(value: AContextable<T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
