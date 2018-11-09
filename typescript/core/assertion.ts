import * as protos from "@dataform/protos";
import { Dataform } from "./index";
import * as utils from "./utils";

export type AContextable<T> = T | ((ctx: AssertionContext) => T);

export class Assertion {
  proto: protos.IAssertion = protos.Assertion.create();

  // Hold a reference to the Dataform instance.
  dataform: Dataform;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: AContextable<string>;

  public query(query: AContextable<string>) {
    this.contextableQuery = query;
    return this;
  }

  public dependencies(value: string | string[]) {
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

    var appliedQuery = context.apply(this.contextableQuery);
    this.proto.query = appliedQuery;
    this.contextableQuery = null;

    // Evaluate wildcard dependencies.
    this.proto.dependencies = utils.matchPatterns(this.proto.dependencies, Object.keys(this.dataform.materializations));

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
    return this.assertion.dataform.ref(name);
  }

  public dependencies(name: string | string[]) {
    this.assertion.dependencies(name);
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
