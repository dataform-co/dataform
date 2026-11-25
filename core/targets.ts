import { JSONObjectStringifier } from "df/common/strings/stringifier";
import { dataform } from "df/protos/ts";

/** Produces an unambigous mapping to and from a string representation. */
export const targetStringifier = new JSONObjectStringifier<dataform.ITarget>();

/**
 * Provides a readable string representation of the target which is used for e.g. specifying
 * actions on the CLI.
 * This is effectively equivelant to an action "name".
 *
 * This is an ambiguous transformation, multiple targets may map to the same string
 * and it should not be used for indexing. Use @see {@link targetStringifier} instead.
 */
export function targetAsReadableString(target: dataform.ITarget): string {
  const nameParts = [target.name, target.schema];
  if (!!target.database) {
    nameParts.push(target.database);
  }
  return nameParts.reverse().join(".");
}

export function hasInvalidProjectColonUsage(database: string): boolean {
  const colonIndex = database.indexOf(":");
  return (
    colonIndex !== -1 &&
    (colonIndex === 0 ||
      colonIndex === database.length - 1 ||
      database.includes(":", colonIndex + 1))
  );
}

export function stripProjectDomainPrefix(database: string): string {
  const colonIndex = database.indexOf(":");
  return colonIndex > 0 ? database.slice(colonIndex + 1) : database;
}

/**
 * Returns true when `schema` is a Lakehouse `catalog.namespace` pair rather than a plain BigQuery
 * dataset. Standard BigQuery dataset names cannot contain `.`, so a dot unambiguously identifies a
 * normalized 4-part Lakehouse target (`project.catalog.namespace.table`).
 */
export function isCatalogSchema(schema?: string | null): boolean {
  return Boolean(schema && schema.includes("."));
}

/**
 * Returns true when `target` refers to a 4-part Lakehouse Catalog object.
 */
export function isLakehouseTarget(target?: dataform.ITarget | null): boolean {
  return isCatalogSchema(target?.schema);
}
