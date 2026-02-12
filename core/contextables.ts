import { dataform } from "df/protos/ts";

/**
 * A resolvable is a reference to an action, and it can be either the string representation of the
 * action's target, or the target of the action.
 */
export type Resolvable = string | dataform.ITarget;

/**
 * Contextable arguments can either pass a plain value for their generic type `T` or can pass a
 * function that will be called with the context object for this type of operation.
 */
export type Contextable<Context, T> = T | ((ctx: Context) => T);

/**
 * Action contexts are available when evaluating contexts in actions, such as within SQLX files, or
 * when using a [Contextable](#Contextable) argument with the JS API.
 */
export interface IActionContext {
  /**
   * Equivelant to `resolve(name())`.
   *
   * Returns a valid SQL string that can be used to reference the dataset produced by this action.
   */
  self: () => string;

  /**
   * Returns the name of this dataset.
   */
  name: () => string;

  /**
   * References another action, adding it as a dependency to this action, returning valid SQL to be used in a `from` expression.
   *
   * This function can be called with a [Resolvable](#Resolvable) object, for example:
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
   * Similar to `ref` except that it does not add a dependency, but just resolves the provided reference
   * so that it can be used in SQL, for example in a `from` expression.
   *
   * See the `ref` function for example usage.
   */
  resolve: (ref: Resolvable | string[], ...rest: string[]) => string;

  /**
   * Returns the schema of this dataset.
   */
  schema: () => string;

  /**
   * Returns the database of this dataset, if applicable.
   */
  database: () => string;
}

/**
 * Table context is are available when evaluating contextable SQL code for actions of type Table,
 * View, or Incremental tables.
 */
export interface ITableContext extends IActionContext {
  /**
   * Shorthand for an `if` condition. Equivalent to `cond ? trueCase : falseCase`.
   * `falseCase` is optional, and defaults to an empty string.
   */
  when: (cond: boolean, trueCase: string, falseCase?: string) => string;

  /**
   * Indicates whether the config indicates the file is dealing with an incremental table.
   */
  incremental: () => boolean;
}

/** JiT context, accessible at JiT compilation stage. */
export type JitContext<T> = T & {
  /** Direct access to adapter. */
  adapter: dataform.DbAdapter,
  /** JiT data object. */
  data?: { [k: string]: any },
  /** Original JiT compilation request. */
  request: dataform.IJitCompilationRequest,
};

/** 
 * JiT contextable - async function that accepts JiT context
 * or raw string with JS code of this function.
 */
export type JitContextable<Context, T> = string | ((jctx: JitContext<Context>) => Promise<T>);
