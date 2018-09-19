import * as protos from "./protos";
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
    this.assertion.proto.dependencies.push(name);
    return this.assertion.dataform.ref(name);
  }

  public apply<T>(value: AContextable<T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
