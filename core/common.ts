import { strictKeysOf } from "df/core/utils";

// TODO(ekrekr): remove deprecated items from this file.

/**
 * Context methods are available when evaluating contextable SQL code, such as
 * within SQLX files, or when using a [Contextable](#Contextable) argument with the JS API.
 */
export interface ICommonContext {
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
 * @hidden
 */
export interface INamedConfig {
  /**
   * The type of the action.
   *
   * @hidden
   */
  type?: string;

  /**
   * The name of the action.
   *
   * @hidden
   */
  name?: string;
}

/**
 * @hidden
 */
export interface IActionConfig {
  /**
   * A list of user-defined tags with which the action should be labeled.
   */
  tags?: string[];

  /**
   * Dependencies of the action.
   *
   * @hidden
   */
  dependencies?: Resolvable | Resolvable[];

  /**
   * If set to true, this action will not be executed. However, the action may still be depended upon.
   * Useful for temporarily turning off broken actions.
   */
  disabled?: boolean;
}

/**
 * @hidden
 */
export interface ITargetableConfig {
  /**
   * The database in which the output of this action should be created.
   */
  database?: string;

  /**
   * The schema in which the output of this action should be created.
   */
  schema?: string;
}

/**
 * @hidden
 */
export interface IDependenciesConfig {
  /**
   * One or more explicit dependencies for this action. Dependency actions will run before dependent actions.
   * Typically this would remain unset, because most dependencies are declared as a by-product of using the `ref` function.
   */
  dependencies?: Resolvable | Resolvable[];

  /**
   * Declares whether or not this action is hermetic. An action is hermetic if all of its dependencies are explicitly
   * declared.
   *
   * If this action depends on data from a source which has not been declared as a dependency, then `hermetic`
   * should be explicitly set to `false`. Otherwise, if this action only depends on data from explicitly-declared
   * dependencies, then it should be set to `true`.
   */
  hermetic?: boolean;

  /**
   * If this flag is set to true, assertions dependent upon any of the dependencies are added as dependencies as well.
   */
  dependOnDependencyAssertions?: boolean;
}

/**
 * @hidden
 */
export interface IDocumentableConfig {
  /**
   * A description of columns within the dataset.
   */
  columns?: IColumnsDescriptor;

  /**
   * A description of the dataset.
   */
  description?: string;
}

/**
 * Describes columns in a dataset.
 */
export interface IColumnsDescriptor {
  [name: string]: string | IRecordDescriptor;
}

/**
 * Describes a struct, object or record in a dataset that has nested columns.
 */
export interface IRecordDescriptor {
  /**
   * A description of the struct, object or record.
   */
  description?: string;

  /**
   * A description of columns within the struct, object or record.
   */
  columns?: IColumnsDescriptor;

  /**
   * @hidden
   */
  displayName?: string;

  /**
   * Tags that apply to this column (experimental).
   *
   * @hidden
   */
  tags?: string | string[];

  /**
   * BigQuery policy tags that should be applied to this column.
   *
   * These should be the fully qualified identifier of the tag, including the project name, location, and taxonomy,
   * which can be copied from the policy tags page in GCP.
   *
   * For example: "projects/1/locations/eu/taxonomies/2/policyTags/3"
   *
   * Currently BigQuery supports only a single tag per column.
   */
  bigqueryPolicyTags?: string | string[];
}

/**
 * @hidden
 *
 * TODO: This needs to be a method, I'm really not sure why, but it hits a runtime failure otherwise.
 */
export const IRecordDescriptorProperties = () =>
  strictKeysOf<IRecordDescriptor>()([
    "description",
    "columns",
    "displayName",
    "tags",
    "bigqueryPolicyTags"
  ]);

/**
 * A reference to a dataset within the warehouse.
 */
export interface ITarget {
  database?: string;

  schema?: string;

  name?: string;

  includeDependentAssertions?: boolean;
}

/**
 * A resolvable can be either the name of a dataset as string, or
 * an object that describes the full path to the relation.
 */
export type Resolvable = string | ITarget;

/**
 * Contextable arguments can either pass a plain value for their
 * generic type `T` or can pass a function that will be called
 * with the context object for this type of operation.
 */
export type Contextable<Context, T> = T | ((ctx: Context) => T);
