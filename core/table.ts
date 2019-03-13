import { Session } from "./index";
import * as protos from "@dataform/protos";

export enum TableTypes {
  TABLE = "table",
  VIEW = "view",
  INCREMENTAL = "incremental"
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

type ValueOf<T> = T[keyof T];
export type TContextable<T> = T | ((ctx: TableContext) => T);
export type TableType = ValueOf<TableTypes>;

export interface TConfig {
  type?: TableType;
  query?: TContextable<string>;
  where?: TContextable<string>;
  preOps?: TContextable<string | string[]>;
  postOps?: TContextable<string | string[]>;
  dependencies?: string | string[];
  descriptor?: string[] | { [key: string]: string };
  disabled?: boolean;
  redshift?: protos.IRedshiftOptions;
  bigquery?: protos.IBigQueryOptions;
}

export class Table {
  proto: protos.Table = protos.Table.create({
    type: "view",
    disabled: false
  });

  // Hold a reference to the Session instance.
  session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQuery: TContextable<string>;
  private contextableWhere: TContextable<string>;
  private contextablePreOps: TContextable<string | string[]>[] = [];
  private contextablePostOps: TContextable<string | string[]>[] = [];

  public config(config: TConfig) {
    if (config.where) {
      this.where(config.where);
    }
    if (config.type) {
      this.type(config.type);
    }
    if (config.query) {
      this.query(config.query);
    }
    if (config.preOps) {
      this.preOps(config.preOps);
    }
    if (config.postOps) {
      this.postOps(config.postOps);
    }
    if (config.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (config.descriptor) {
      if (config.descriptor instanceof Array) {
        this.descriptor(config.descriptor);
      } else {
        this.descriptor(config.descriptor);
      }
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

  public redshift(redshift: protos.IRedshiftOptions) {
    this.proto.redshift = protos.RedshiftOptions.create(redshift);
    return this;
  }

  public bigquery(bigquery: protos.IBigQueryOptions) {
    this.proto.bigquery = protos.BigQueryOptions.create(bigquery);
    return this;
  }

  private addDependency(dependency: string): void {
    if (this.proto.dependencies.indexOf(dependency) < 0) {
      this.proto.dependencies.push(dependency);
    }
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

  public descriptor(key: string, description?: string): Table;
  public descriptor(map: { [key: string]: string }): Table;
  public descriptor(keys: string[]): Table;
  public descriptor(keyOrKeysOrMap: string | string[] | { [key: string]: string }, description?: string): Table {
    if (!this.proto.fieldDescriptor) {
      this.proto.fieldDescriptor = {};
    }
    if (typeof keyOrKeysOrMap === "string") {
      this.proto.fieldDescriptor[keyOrKeysOrMap] = description || "";
    } else if (keyOrKeysOrMap instanceof Array) {
      keyOrKeysOrMap.forEach(key => {
        this.proto.fieldDescriptor[key] = "";
      });
    } else {
      Object.keys(keyOrKeysOrMap).forEach(key => {
        this.proto.fieldDescriptor[key] = keyOrKeysOrMap[key] || "";
      });
    }
    return this;
  }

  compile() {
    var context = new TableContext(this);

    this.proto.query = context.apply(this.contextableQuery);
    this.contextableQuery = null;

    if (this.contextableWhere) {
      this.proto.where = context.apply(this.contextableWhere);
      this.contextableWhere = null;
    }

    this.contextablePreOps.forEach(contextablePreOps => {
      var appliedPres = context.apply(contextablePreOps);
      this.proto.preOps = (this.proto.preOps || []).concat(
        typeof appliedPres == "string" ? [appliedPres] : appliedPres
      );
    });
    this.contextablePreOps = [];

    this.contextablePostOps.forEach(contextablePostOps => {
      var appliedPosts = context.apply(contextablePostOps);
      this.proto.postOps = (this.proto.postOps || []).concat(
        typeof appliedPosts == "string" ? [appliedPosts] : appliedPosts
      );
    });
    this.contextablePostOps = [];

    return this.proto;
  }
}

export class TableContext {
  private table?: Table;

  constructor(table: Table) {
    this.table = table;
  }

  public config(config: TConfig) {
    this.table.config(config);
    return "";
  }

  public self(): string {
    return this.table.session.adapter().resolveTarget(this.table.proto.target);
  }

  public ref(name: string) {
    this.table.dependencies(name);
    return this.table.session.ref(name);
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

  public redshift(redshift: protos.IRedshiftOptions) {
    this.table.redshift(redshift);
    return "";
  }

  public bigquery(bigquery: protos.IBigQueryOptions) {
    this.table.bigquery(bigquery);
    return "";
  }

  public dependencies(name: string) {
    this.table.dependencies(name);
    return "";
  }

  public descriptor(key: string, description?: string): string;
  public descriptor(map: { [key: string]: string }): string;
  public descriptor(keys: string[]): string;
  public descriptor(keyOrKeysOrMap: string | string[] | { [key: string]: string }, description?: string): string {
    this.table.descriptor(keyOrKeysOrMap as any, description);
    return "";
  }

  public describe(key: string, description?: string) {
    this.table.descriptor(key, description);
    return key;
  }

  public apply<T>(value: TContextable<T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
