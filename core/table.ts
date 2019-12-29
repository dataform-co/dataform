import {
  IColumnsDescriptor,
  mapToColumnProtoArray,
  Resolvable,
  Session
} from "@dataform/core/session";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

/**
 * @hidden
 */
export const TableType = ["table", "view", "incremental", "inline"] as const;
export type TableType = typeof TableType[number];

/**
 * @hidden
 */
export const DistStyleType = ["even", "key", "all"] as const;
export type DistStyleType = typeof DistStyleType[number];

/**
 * @hidden
 */
export const SortStyleType = ["compound", "interleaved"] as const;
export type SortStyleType = typeof SortStyleType[number];

/**
 * @hidden
 */
export const ignoredProps: {
  [tableType: string]: Array<keyof dataform.ITable>;
} = {
  inline: [
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

export type TContextable<T> = T | ((ctx: TableContext) => T);

// For documentation purposes, there should be no protos in public interfaces for the SQLX or JS API.
// The following interfaces should be kept up to date and documented in line with protobuf interfaces.

/**
 * Redshift specific warehouse options.
 */
export interface IRedshiftOptions {
  /**
   * Sets the DISTKEY property when creating tables.
   *
   * For more information, read the AWS documentation [here](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html#r_CREATE_TABLE_NEW-diststyle-distkey-and-sortkey-options).
   */
  distKey?: string;
  distStyle?: string;
  sortKeys?: string[];
  sortStyle?: string;
  bind?: boolean;
}

export interface ISQLDataWarehouseOptions {
  distribution?: string;
}

export interface IBigQueryOptions {
  /**
   * Add some description.
   */
  partitionBy?: string;
}

/**
 * Configuration options that can be provided to a table.
 */
export interface ITableConfig {
  type?: TableType;
  dependencies?: Resolvable | Resolvable[];
  tags?: string[];
  description?: string;
  columns?: IColumnsDescriptor;
  disabled?: boolean;
  protected?: boolean;
  redshift?: IRedshiftOptions;
  bigquery?: IBigQueryOptions;
  sqldatawarehouse?: ISQLDataWarehouseOptions;
  database?: string;
  schema?: string;
}

/**
 * @hidden
 */
export class Table {
  public proto: dataform.ITable = dataform.Table.create({
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

  public config(config: ITableConfig) {
    if (config.type) {
      this.type(config.type);
    }
    if (config.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (config.disabled) {
      this.disabled();
    }
    if (config.protected) {
      this.protected();
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
    if (config.database) {
      this.database(config.database);
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

  public protected() {
    this.proto.protected = true;
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

  public database(database: string) {
    utils.setNameAndTarget(
      this.session,
      this.proto,
      this.proto.target.name,
      this.proto.target.schema,
      database
    );
    return this;
  }

  public schema(schema: string) {
    utils.setNameAndTarget(
      this.session,
      this.proto,
      this.proto.target.name,
      schema,
      this.proto.target.database
    );
    return this;
  }

  public compile() {
    const context = new TableContext(this);
    const incrementalContext = new TableContext(this, true);

    this.proto.query = context.apply(this.contextableQuery);
    if (this.proto.type === "incremental") {
      this.proto.incrementalQuery = incrementalContext.apply(this.contextableQuery);
    }

    if (this.contextableWhere) {
      this.proto.where = context.apply(this.contextableWhere);
    }

    this.contextablePreOps.forEach(contextablePreOps => {
      const appliedPres = context.apply(contextablePreOps);
      this.proto.preOps = (this.proto.preOps || []).concat(
        typeof appliedPres === "string" ? [appliedPres] : appliedPres
      );
    });
    this.contextablePreOps = [];

    this.contextablePostOps.forEach(contextablePostOps => {
      const appliedPosts = context.apply(contextablePostOps);
      this.proto.postOps = (this.proto.postOps || []).concat(
        typeof appliedPosts === "string" ? [appliedPosts] : appliedPosts
      );
    });
    this.contextablePostOps = [];
    return this.proto;
  }
}

export interface ITableContext {
  /**
   * Returns a valid SQL string that can be used to reference the dataset produced by this action.
   */
  self: () => string;

  /**
   * Returns the name of this dataset.
   */
  name: () => string;

  /**
   * References another action, returning valid SQL to be used in a `from` expression and adds it as a dependency to this action.
   *
   * This function can be called with a [[Resolvable]] object, for example:
   *
   * ```typescript
   * ${ref({ name: "name", schema: "schema", database: "database" })}
   * ```
   *
   * This function can also be called using individual arguments for the "database", "schema", and "name" values.
   * When only two values are provided, the default database will be used and the values will be interpreted as "schema" and "name".
   * When only one value is provided, the default data base schema will be used, with the provided value interpreted as "name".
   *
   * ```typescript
   * ${ref("database", "schema", "name")}
   * ${ref("schema", "name")}
   * ${ref("name")}
   * ```
   */
  ref: (ref: Resolvable | string[], ...rest: string[]) => string;

  /**
   * Similar to <code>ref</code> except that it does not add a dependency, but simply resolves the provided reference.
   * See <code>ref</code> for usage.
   */
  resolve: (ref: Resolvable | string[], ...rest: string[]) => string;

  /**
   * @hidden
   */
  isIncremental: () => boolean;

  /**
   * @hidden
   */
  ifIncremental: (value: string) => string;

  // The following methods are deprecated since SQLX and will be removed in a future version of Dataform.

  /**
   * @deprecated
   * @hidden
   */
  config: (config: ITableConfig) => string;

  /**
   * @deprecated
   * @hidden
   */
  type: (type: TableType) => string;

  /**
   * @deprecated
   * @hidden
   */
  where: (where: TContextable<string>) => string;

  /**
   * @deprecated
   * @hidden
   */
  preOps: (statement: TContextable<string | string[]>) => string;

  /**
   * @deprecated
   * @hidden
   */
  postOps: (statement: TContextable<string | string[]>) => string;

  /**
   * @deprecated
   * @hidden
   */
  disabled: () => string;

  /**
   * @deprecated
   * @hidden
   */
  redshift: (redshift: dataform.IRedshiftOptions) => string;

  /**
   * @deprecated
   * @hidden
   */
  bigquery: (bigquery: dataform.IBigQueryOptions) => string;

  /**
   * @deprecated
   * @hidden
   */
  dependencies: (name: Resolvable) => string;

  /**
   * @hidden
   */
  apply: <T>(value: TContextable<T>) => T;

  /**
   * @deprecated
   * @hidden
   */
  tags: (name: string | string[]) => string;
}

/**
 * @hidden
 */
export class TableContext implements ITableContext {
  constructor(private table: Table, private incremental = false) {}

  public config(config: ITableConfig) {
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
    return this.table.proto.target.name;
  }

  public ref(ref: Resolvable | string[], ...rest: string[]): string {
    ref = utils.toResolvable(ref, rest);
    if (!utils.resolvableAsTarget(ref)) {
      const message = `Action name is not specified`;
      this.table.session.compileError(new Error(message));
      return "";
    }
    this.table.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.table.session.resolve(utils.toResolvable(ref, rest));
  }

  public type(type: TableType) {
    this.table.type(type);
    return "";
  }

  public where(where: TContextable<string>) {
    this.table.where(where);
    return "";
  }

  public isIncremental() {
    return !!this.incremental;
  }

  public ifIncremental(value: string) {
    return this.isIncremental() ? value : "";
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

  public dependencies(res: Resolvable) {
    this.table.dependencies(res);
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
