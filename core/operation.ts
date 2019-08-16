import {
  IColumnsDescriptor,
  mapToColumnProtoArray,
  Resolvable,
  Session
} from "@dataform/core/session";
import { dataform } from "@dataform/protos";

export type OContextable<T> = T | ((ctx: OperationContext) => T);

export interface OConfig {
  dependencies?: string | string[];
  tags?: string[];
  description?: string;
  columns?: IColumnsDescriptor;
  hasOutput?: boolean;
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
    if (config.schema) {
      this.schema(config.schema);
    }
    return this;
  }

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

  public schema(schema: string) {
    if (this.session.findActions({ schema: schema, name: this.proto.target.name }).length === 0) {
      this.proto.target.schema = schema;
      this.proto.name = `${schema}.${this.proto.target.name}`;
    } else {
      const message = `Duplicate action name detected. Names within a schema must be unique across tables, assertions, and operations: "${name}"`;
      this.session.compileError(new Error(message));
    }
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
    return this.operation.proto.name;
  }

  public ref(ref: Resolvable) {
    const name =
      typeof ref === "string" || typeof ref === "undefined" ? ref : `${ref.schema}.${ref.name}`;
    this.operation.dependencies(name);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable) {
      return this.operation.session.resolve(ref);
  }

  public dependencies(name: string | string[]) {
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
