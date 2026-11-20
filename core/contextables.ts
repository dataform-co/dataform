import { dataform } from "df/protos/ts";

/**
 * Object representation of an action target accepted by `ref()` and `resolve()`.
 */
export interface IResolvable {
  database?: string;
  project?: string;
  schema?: string;
  dataset?: string;
  catalog?: string;
  namespace?: string;
  lakehouse?: dataform.ActionConfig.ILakehouseConfig;
  name?: string;
  includeDependentAssertions?: boolean;
}

/**
 * A resolvable is a reference to an action, and it can be either the string representation of the
 * action's target, or the target of the action.
 */
export type Resolvable = string | dataform.ITarget | IResolvable;

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
   * Resolves an action target to its backtick-quoted SQL identifier and registers it as an upstream dependency.
   *
   * This function can be called with a [Resolvable](#Resolvable) object, for example:
   *
   * ```typescript
   * ${ref({ name: "name", schema: "schema", database: "database" })}
   * ```
   *
   * This function can also be called using individual arguments for the "database", "schema", and "name" values.
   * When only two values are provided, the default database will be used and the values will be interpreted as "schema" and "name".
   * When only one value is provided, the default database and schema will be used, with the provided value interpreted as "name".
   *
   * ```typescript
   * ${ref("database", "schema", "name")}
   * ${ref("schema", "name")}
   * ${ref("name")}
   * ```
   *
   * @signature `ref(name)` / `ref(schema, name)` / `ref(database, schema, name)` / `ref({ database?, schema?, name, includeDependentAssertions? })`
   */
  ref: (ref: Resolvable | string[], ...rest: string[]) => string;

  /**
   * Resolves an action target to its backtick-quoted SQL identifier **without** adding it as a dependency.
   *
   * See the `ref` function for example usage.
   *
   * @signature `resolve(name)` / `resolve(schema, name)` / `resolve(database, schema, name)` / `resolve(targetObj)`
   */
  resolve: (ref: Resolvable | string[], ...rest: string[]) => string;

  /**
   * Returns the fully qualified, backtick-quoted SQL identifier of the current action's target.
   *
   * Equivalent to `resolve(name())`.
   */
  self: () => string;

  /**
   * Returns the finalized table/action name of the current action (including `namePrefix` if set).
   */
  name: () => string;

  /**
   * Returns the finalized dataset (`schema`) name of the current action (including `datasetSuffix` if set).
   */
  schema: () => string;

  /**
   * Returns the finalized Google Cloud project (`database`) of the current action (including `projectSuffix` if set).
   */
  database: () => string;

  /**
   * Returns `trueCase` when `condition` is truthy and `falseCase` otherwise.
   *
   * Equivalent to `condition ? trueCase : falseCase`.
   *
   * @signature `when(condition, trueCase, falseCase = "")`
   */
  when: (condition: boolean, trueCase: string, falseCase?: string) => string;
}

/**
 * Table context is available when evaluating contextable SQL code for actions of type Table,
 * View, Incremental, or DataPreparation.
 */
export interface ITableContext extends IActionContext {
  /**
   * Returns `true` when compiling the incremental update query of an `incremental` table, or `false` when compiling its initial/full-refresh query (`table`, `incremental`, `view`, `dataPreparation`).
   */
  incremental: () => boolean;
}

/** JiT context, accessible at JiT compilation stage. */
export type JitContext<T> = T & {
  /** Direct access to adapter. */
  adapter: dataform.DbAdapter;
  /** JiT data object. */
  data?: { [k: string]: any };
  /** Original JiT compilation request. */
  request: dataform.IJitCompilationRequest;
  /** Current execution information for introspection. */
  executionData: dataform.IRunningExecutionData;
};

/**
 * JiT contextable - async function that accepts JiT context
 * or raw string with JS code of this function.
 */
export type JitContextable<Context, T> = string | ((jctx: JitContext<Context>) => Promise<T>);
