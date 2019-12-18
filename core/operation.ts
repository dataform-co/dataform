import {
  IColumnsDescriptor,
  mapToColumnProtoArray,
  Resolvable,
  Session
} from "@dataform/core/session";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

export type OContextable<T> = T | ((ctx: OperationContext) => T);

export interface OConfig {
  dependencies?: Resolvable | Resolvable[];
  tags?: string[];
  description?: string;
  columns?: IColumnsDescriptor;
  hasOutput?: boolean;
  database?: string;
  schema?: string;
}

export class Operation {
  public proto: dataform.IOperation = dataform.Operation.create();

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQueries: OContextable<string | string[]>;

  public config(config: OConfig) {
    if (config.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (config.tags) {
      this.tags(config.tags);
    }
    if (config.hasOutput) {
      this.hasOutput(config.hasOutput);
    }
    if (config.description) {
      this.description(config.description);
    }
    if (config.columns) {
      this.columns(config.columns);
    }
    if (config.database) {
      this.database(config.database);
    }
    if (config.schema) {
      this.schema(config.schema);
    }
    return this;
  }

  public queries(queries: OContextable<string | string[]>) {
    this.contextableQueries = queries;
    return this;
  }

  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
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

  public hasOutput(hasOutput: boolean) {
    this.proto.hasOutput = hasOutput;
    return this;
  }

  public description(description: string) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.description = description;
    return this;
  }

  public columns(columns: IColumnsDescriptor) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.columns = mapToColumnProtoArray(columns);
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
    if (
      this.proto.actionDescriptor &&
      this.proto.actionDescriptor.columns &&
      this.proto.actionDescriptor.columns.length > 0 &&
      !this.proto.hasOutput
    ) {
      this.session.compileError(
        new Error(
          "Actions of type 'operations' may only describe columns if they specify 'hasOutput: true'."
        ),
        this.proto.fileName
      );
    }

    const context = new OperationContext(this);

    const appliedQueries = context.apply(this.contextableQueries);
    this.proto.queries = typeof appliedQueries === "string" ? [appliedQueries] : appliedQueries;

    return this.proto;
  }
}

export class OperationContext {
  private operation?: Operation;

  constructor(operation: Operation) {
    this.operation = operation;
  }

  public self(): string {
    return this.resolve({
      schema: this.operation.proto.target.schema,
      name: this.operation.proto.target.name
    });
  }

  public name(): string {
    return this.operation.proto.target.name;
  }

  public ref(ref: Resolvable) {
    if (!utils.resolvableAsTarget(ref)) {
      const message = `Action name is not specified`;
      this.operation.session.compileError(new Error(message));
      return "";
    }
    this.operation.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable) {
    return this.operation.session.resolve(ref);
  }

  public dependencies(name: Resolvable | Resolvable[]) {
    this.operation.dependencies(name);
    return "";
  }

  public tags(name: string | string[]) {
    this.operation.tags(name);
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
