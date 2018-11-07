import * as protos from "@dataform/protos";
import { Dataform } from "./index";

export type AContextable<T> = T | ((ctx: AssertionContext) => T);

export class Assertion {
  proto: protos.IAssertion = protos.Assertion.create();

  // Hold a reference to the Dataform instance.
  dataform: Dataform;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQueries: AContextable<string | string[]>;

  public query(query: AContextable<string | string[]>) {
    this.contextableQueries = query;
  }

  public dependency(value: string | string[]) {
    var newDependencies = typeof value === "string" ? [value] : value;
    newDependencies.forEach(d => {
      if (this.proto.dependencies.indexOf(d) < 0) {
        this.proto.dependencies.push(d);
      }
    });
    return this;
  }

  compile() {
    var context = new AssertionContext(this);

    var appliedQueries = context.apply(this.contextableQueries);
    this.proto.queries =
      typeof appliedQueries == "string" ? [appliedQueries] : appliedQueries;
    this.contextableQueries = null;

    return this.proto;
  }
}

export class AssertionContext {
  private assertion?: Assertion;

  constructor(assertion: Assertion) {
    this.assertion = assertion;
  }

  public ref(name: string) {
    this.assertion.dependency(name);
    return this.assertion.dataform.ref(name);
  }

  public dependency(name: string | string[]) {
    this.assertion.dependency(name);
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
