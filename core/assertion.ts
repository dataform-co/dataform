import { mapToDescriptorProto, Session } from "@dataform/core/session";
import { dataform } from "@dataform/protos";

export type AContextable<T> = T | ((ctx: AssertionContext) => T);

export class Assertion {
  public proto: dataform.IAssertion = dataform.Assertion.create();

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: AContextable<string>;

  public query(query: AContextable<string>) {
    this.contextableQuery = query;
    return this;
  }

  public dependencies(value: string | string[]) {
    const newDependencies = typeof value === "string" ? [value] : value;
    newDependencies.forEach(d => {
      if (this.proto.dependencies.indexOf(d) < 0) {
        this.proto.dependencies.push(d);
      }
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

  public describe(description: string) {
    this.proto.actionDescriptor = mapToDescriptorProto({ description });
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

  public ref(name: string) {
    this.assertion.dependencies(name);
    return this.resolve(name);
  }

  public resolve(name: string) {
    return this.assertion.session.resolve(name);
  }

  public dependencies(name: string | string[]) {
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
