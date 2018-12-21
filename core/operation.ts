import * as protos from "@dataform/protos";
import { Session } from "./index";
import * as utils from "./utils";

export type OContextable<T> = T | ((ctx: OperationContext) => T);

export class Operation {
  proto: protos.IOperation = protos.Operation.create({
    hasOutput: false
  });

  // Hold a reference to the Session instance.
  session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQueries: OContextable<string | string[]>;

  public queries(queries: OContextable<string | string[]>) {
    this.contextableQueries = queries;
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

  public hasOutput(hasOutput: boolean) {
    this.proto.hasOutput = hasOutput;
    return this;
  }

  compile() {
    var context = new OperationContext(this);

    var appliedQueries = context.apply(this.contextableQueries);
    this.proto.queries = typeof appliedQueries == "string" ? [appliedQueries] : appliedQueries;
    this.contextableQueries = null;

    console.log('**** ****', this.proto);
    return this.proto;
  }
}

export class OperationContext {
  private operation?: Operation;

  constructor(operation: Operation) {
    this.operation = operation;
  }

  public ref(name: string) {
    this.operation.dependencies(name);
    return this.operation.session.ref(name);
  }

  public dependencies(name: string | string[]) {
    this.operation.dependencies(name);
    return "";
  }

  public hasOutput(hasOutput: boolean) {
    this.operation.hasOutput(hasOutput);
    return "";
  }

  public apply<T>(value: OContextable<T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
