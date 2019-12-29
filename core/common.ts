/**
 * Context methods are available when evaluating contextable SQL code, such as
 * within SQLX files, or when using a [Contextable](#Contextable) argument with the JS API.
 * 
 * @hidden
 */
export interface ICommonContext {
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
}

export interface ICommonConfig {
  /**
   * One or more explicit dependencies for the table action. This is typically not needed, as it is inferred from using
   * the `ref` function within SQLX files.
   */
  dependencies?: Resolvable | Resolvable[];

  /**
   * A list of tags that should be applied to this action. This is useful for managing large projects.
   */
  tags?: string[];
}

export interface ICommonOutputConfig extends ICommonConfig {
  /**
   * The database (or Google Cloud project ID) for this dataset.
   */
  database?: string;

  /**
   * The schema (or dataset, in BigQuery) for this dataset (or table / view, in BigQuery).
   */
  schema?: string;

  /**
   * A description of the dataset that will be used to populate the data catalog.
   */
  description?: string;

  /**
   * A descriptor for columns within the dataset.
   */
  columns?: IColumnsDescriptor;
}

/**
 * Describes columns in a dataset, used for populating the data catalog.
 */
export interface IColumnsDescriptor {
  [name: string]: string | IRecordDescriptor;
}

/**
 * Describes a struct, object or record in a dataset that has nested columns.
 */
export interface IRecordDescriptor {
  description?: string;
  columns?: IColumnsDescriptor;
}

/**
 * A reference to a dataset within the warehouse.
 */
export interface ITarget {
  database?: string;

  schema?: string;

  name?: string;
}

/**
 * A resolvable can be either the name of a dataset as string, or
 * an object that describes the full path to the relation.
 */
export type Resolvable = string | ITarget;
