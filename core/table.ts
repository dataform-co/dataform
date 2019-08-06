import { IDatasetDescriptor, mapToDescriptorProto, Session } from "@dataform/core/session";
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
    "preOps",
    "postOps",
    "descriptor",
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
  columns?: IDatasetDescriptor;
  disabled?: boolean;
  protected?: boolean;
  redshift?: dataform.IRedshiftOptions;
  bigquery?: dataform.IBigQueryOptions;
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
    if (config.tags) {
      this.tags(config.tags);
    }
    if (config.description || config.columns) {
      if (config.description) {
        this.describe(config.description, config.columns);
      } else {
        this.describe(config.columns);
      }
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
      const table = this.session.tables[d];

      if (!!table && table.proto.type === "inline") {
        table.proto.dependencies.forEach(childDep => this.addDependency(childDep));
      } else {
        this.addDependency(d);
      }
    });
    return this;
  }

  public tags(value: string | string[]) {
    const newTags = typeof value === "string" ? [value] : value;
    newTags.forEach(t => {
      const table = this.session.tables[t];
      this.proto.tags.push(t);
    });
    return this;
  }

  public describe(description: string | IDatasetDescriptor, descriptor?: IDatasetDescriptor) {
    if (typeof description === "string") {
      this.proto.descriptor = mapToDescriptorProto(description, descriptor);
    } else {
      this.proto.descriptor = mapToDescriptorProto(undefined, description);
    }
    return this;
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
    return this.resolve(this.table.proto.name);
  }

  public name(): string {
    return this.table.proto.name;
  }

  public ref(name: string) {
    if (!name) {
      const message = `Action name is not specified`;
      this.table.session.compileError(new Error(message));
      return "";
    }

    this.table.dependencies(name);
    return this.resolve(name);
  }

  public resolve(name: string) {
    return this.table.session.resolve(name);
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
