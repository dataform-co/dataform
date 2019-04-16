import { dataform } from "@dataform/protos";
import { Session } from "./index";
import * as utils from "./utils";

export type OContextable<T> = T | ((ctx: OperationContext) => T);

export class Operation {
  public proto: dataform.IOperation = dataform.Operation.create();

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQueries: OContextable<string | string[]>;

  public queries(queries: OContextable<string | string[]>) {
    this.contextableQueries = queries;
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

  public hasOutput(hasOutput: boolean) {
    this.proto.hasOutput = hasOutput;
    return this;
  }

  public compile() {
    const context = new OperationContext(this);

    const appliedQueries = context.apply(this.contextableQueries);
    this.proto.queries = typeof appliedQueries == "string" ? [appliedQueries] : appliedQueries;
    this.contextableQueries = null;

    return this.proto;
  }
}

export class OperationContext {
  private operation?: Operation;

  constructor(operation: Operation) {
    this.operation = operation;
  }

  public self(): string {
    return this.resolve(this.operation.proto.name);
  }

  public ref(name: string) {
    this.operation.dependencies(name);
    return this.resolve(name);
  }

  public resolve(name: string) {
    return this.operation.session.resolve(name);
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
