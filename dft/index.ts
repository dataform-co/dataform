export class Global {
  nodes: Node[] = [];
}

export interface Config {
  warehouse: Warehouse;
}

export const GLOBAL = new Global();

export class Relation {
  name: string;
  schema?: string;

  constructor(name: string, schema?: string) {
    this.name = name;
    this.schema = schema;
  }

  public fullName(): string {
    return `"${this.schema}"."${this.name}"`;
  }
}

export type Warehouse = "bigquery" | "redshift";
export type MaterializationType = "table" | "view" | "incremental";
export type Contextable<T> = T | ((ctx: Context) => T);

export interface Node {
  _name: string;
  _dependencies?: string[];
}

export class Materialization implements Node {
  _name: string;
  _dependencies?: string[] = [];

  _type: MaterializationType = "view";
  _destination: Relation;
  _query: string;
  _preHook?: string[];
  _postHook?: string[];
  _where?: string;

  public type(type: MaterializationType) {
    this._type = type;
  }

  public query(value: Contextable<string>) {
    this._query = new Context(this).apply(value);
    return this;
  }

  public preHook(value: Contextable<string | string[]>) {
    var appliedValue = new Context(this).apply(value);
    this._preHook = typeof appliedValue === "string" ? [appliedValue] : appliedValue;
    return this;
  }

  public postHook(value: Contextable<string | string[]>) {
    var appliedValue = new Context(this).apply(value);
    this._postHook = typeof appliedValue === "string" ? [appliedValue] : appliedValue;
    return this;
  }

  public runAfter(value: string) {
    this._dependencies.push(value);
    return this;
  }
}

export class Operation implements Node {
  _name: string;
  _dependencies?: string[];

  _statements: string[];
}

export class Context {
  private node?: Materialization;

  constructor(node: Materialization) {
    this.node = node;
  }

  public this(): string {
    if (this.node instanceof Materialization) {
      return this.node._destination.fullName();
    } else {
      return null;
    }
  }

  public ref(name: string) {
    this.node._dependencies.push(name);
    return new Relation(name).fullName();
  }

  public preHook(statement: Contextable<string>) {
    this.node.preHook(statement);
    return "";
  }

  public postHook(statement: Contextable<string>) {
    this.node.postHook(statement);
    return "";
  }

  public runAfter(stage: string) {
    this.node._dependencies.push(stage);
  }

  public apply<T>(value: Contextable<T>): T {
    if (typeof value === "string") {
      return value;
    } else {
      return (value as any)(this);
    }
  }
}

export function materialize(destination: string) {
  var materialization = new Materialization();
  materialization._destination = new Relation(destination);
  // Add it to global index.
  GLOBAL.nodes.push(materialization);
  return materialization;
}
