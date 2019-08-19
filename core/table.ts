import {
  IColumnsDescriptor,
  Resolvable,
  mapToColumnProtoArray,
  Session
} from "@dataform/core/session";
import { dataform } from "@dataform/protos";

export enum TableTypes {
  TABLE = "table",
  VIEW = "view",
  INCREMENTAL = "incremental",
  INLINE = "inline"
}
export enum DistStyleTypes {
  EVEN = "even",
  KEY = "key",
  ALL = "all"
}
export enum SortStyleTypes {
  COMPOUND = "compound",
  INTERLEAVED = "interleaved"
}

export const ignoredProps: {
  [tableType: string]: Array<keyof dataform.ITable>;
} = {
  [TableTypes.INLINE]: [
    "bigquery",
    "redshift",
    "sqlDataWarehouse",
    "preOps",
    "postOps",
    "actionDescriptor",
    "disabled",
    "where"
  ]
};

type ValueOf<T> = T[keyof T];
export type TContextable<T> = T | ((ctx: TableContext) => T);
export type TableType = ValueOf<TableTypes>;

export interface TConfig {
  type?: TableType;
  dependencies?: string | string[];
  tags?: string[];
  description?: string;
  columns?: IColumnsDescriptor;
  disabled?: boolean;
  protected?: boolean;
  redshift?: dataform.IRedshiftOptions;
  bigquery?: dataform.IBigQueryOptions;
  sqldatawarehouse?: dataform.ISQLDataWarehouseOptions;
  schema?: string;
}

export class Table {
  public proto: dataform.Table = dataform.Table.create({
    type: "view",
    disabled: false,
    tags: []
  });

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  public contextableQuery: TContextable<string>;
  private contextableWhere: TContextable<string>;
  private contextablePreOps: Array<TContextable<string | string[]>> = [];
  private contextablePostOps: Array<TContextable<string | string[]>> = [];

  public config(config: TConfig) {
    if (config.type) {
      this.type(config.type);
    }
    if (config.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (config.disabled) {
      this.disabled();
    }
    if (config.redshift) {
      this.redshift(config.redshift);
    }
    if (config.bigquery) {
      this.bigquery(config.bigquery);
    }
    if (config.sqldatawarehouse) {
      this.sqldatawarehouse(config.sqldatawarehouse);
    }
    if (config.tags) {
      this.tags(config.tags);
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

  public type(type: TableType) {
    this.proto.type = type as string;
    return this;
  }

  public query(query: TContextable<string>) {
    this.contextableQuery = query;
    return this;
  }

  public where(where: TContextable<string>) {
    this.contextableWhere = where;
    return this;
  }

  public preOps(pres: TContextable<string | string[]>) {
    this.contextablePreOps.push(pres);
    return this;
  }

  public postOps(posts: TContextable<string | string[]>) {
    this.contextablePostOps.push(posts);
    return this;
  }

  public disabled() {
    this.proto.disabled = true;
    return this;
  }

  public sqldatawarehouse(sqlDataWarehouse: dataform.ISQLDataWarehouseOptions) {
    this.proto.sqlDataWarehouse = dataform.SQLDataWarehouseOptions.create(sqlDataWarehouse);
    return this;
  }

  public redshift(redshift: dataform.IRedshiftOptions) {
    this.proto.redshift = dataform.RedshiftOptions.create(redshift);
    return this;
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    this.proto.bigquery = dataform.BigQueryOptions.create(bigquery);
    return this;
  }

  public dependencies(value: string | string[]) {
    const newDependencies = typeof value === "string" ? [value] : value;
    newDependencies.forEach(d => {
      // TODO: This code fails to function correctly if the inline table has not yet
      // been attached to the session. This code probably needs to be moved to compile().
      const allResolved = this.session.findActions(d);
      const resolved = allResolved.length > 0 ? allResolved[0] : undefined;
      if (!!resolved && resolved instanceof Table && resolved.proto.type === "inline") {
        resolved.proto.dependencies.forEach(childDep => this.addDependency(childDep));
      } else {
        this.addDependency(d);
      }
    });
    return this;
  }

  public tags(value: string | string[]) {
    const newTags = typeof value === "string" ? [value] : value;
    newTags.forEach(t => {
      this.proto.tags.push(t);
    });
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
      //TODO: We need to add a test case to catch this specific case, right now it is not tested
      const message = `Duplicate action name detected. Names within a schema must be unique across tables, assertions, and operations: "${this.proto.target.name}"`;
      this.session.compileError(new Error(message));
    }
  }

  public compile() {
    const context = new TableContext(this);

    this.proto.query = context.apply(this.contextableQuery);

    if (this.contextableWhere) {
      this.proto.where = context.apply(this.contextableWhere);
    }

    this.contextablePreOps.forEach(contextablePreOps => {
      const appliedPres = context.apply(contextablePreOps);
      this.proto.preOps = (this.proto.preOps || []).concat(
        typeof appliedPres == "string" ? [appliedPres] : appliedPres
      );
    });
    this.contextablePreOps = [];

    this.contextablePostOps.forEach(contextablePostOps => {
      const appliedPosts = context.apply(contextablePostOps);
      this.proto.postOps = (this.proto.postOps || []).concat(
        typeof appliedPosts == "string" ? [appliedPosts] : appliedPosts
      );
    });
    this.contextablePostOps = [];

    //Check dependencies are not ambiguous
    this.proto.dependencies.forEach(dep => {
      const allResolved = this.session.findActions(dep);
      if (!!allResolved && allResolved.length > 1) {
        const message =
          "Ambiguous Action name: " +
          dep +
          ". Did you mean one of: [" +
          allResolved.map(r => `${r.proto.target.schema}.${r.proto.target.name}`).join(", ") +
          "].";
        this.session.compileError(new Error(message));
      }
    });
    return this.proto;
  }

  private addDependency(dependency: string): void {
    if (this.proto.dependencies.indexOf(dependency) < 0) {
      this.proto.dependencies.push(dependency);
    }
  }
}

export interface ITableContext {
  config: (config: TConfig) => string;
  self: () => string;
  name: () => string;
  ref: (name: string) => string;
  resolve: (name: string) => string;
  type: (type: TableType) => string;
  where: (where: TContextable<string>) => string;
  preOps: (statement: TContextable<string | string[]>) => string;
  postOps: (statement: TContextable<string | string[]>) => string;
  disabled: () => string;
  redshift: (redshift: dataform.IRedshiftOptions) => string;
  bigquery: (bigquery: dataform.IBigQueryOptions) => string;
  dependencies: (name: string) => string;
  apply: <T>(value: TContextable<T>) => T;
  tags: (name: string | string[]) => string;
}

export class TableContext implements ITableContext {
  private table?: Table;

  constructor(table: Table) {
    this.table = table;
  }

  public config(config: TConfig) {
    this.table.config(config);
    return "";
  }

  public self(): string {
    return this.resolve({
      schema: this.table.proto.target.schema,
      name: this.table.proto.target.name
    });
  }

  public name(): string {
    return this.table.proto.name;
  }

  public ref(ref: Resolvable) {
    const name =
      typeof ref === "string" || typeof ref === "undefined" ? ref : `${ref.schema}.${ref.name}`;
    if (!name) {
      const message = `Action name is not specified`;
      this.table.session.compileError(new Error(message));
      return "";
    }
    this.table.dependencies(name);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable) {
    return this.table.session.resolve(ref);
  }

  public type(type: TableType) {
    this.table.type(type);
    return "";
  }

  public where(where: TContextable<string>) {
    this.table.where(where);
    return "";
  }

  public preOps(statement: TContextable<string | string[]>) {
    this.table.preOps(statement);
    return "";
  }

  public postOps(statement: TContextable<string | string[]>) {
    this.table.postOps(statement);
    return "";
  }

  public disabled() {
    this.table.disabled();
    return "";
  }

  public redshift(redshift: dataform.IRedshiftOptions) {
    this.table.redshift(redshift);
    return "";
  }

  public bigquery(bigquery: dataform.IBigQueryOptions) {
    this.table.bigquery(bigquery);
    return "";
  }

  public dependencies(name: string) {
    this.table.dependencies(name);
    return "";
  }

  public apply<T>(value: TContextable<T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
  public tags(tags: string[]) {
    this.table.tags(tags);
    return "";
  }
}
