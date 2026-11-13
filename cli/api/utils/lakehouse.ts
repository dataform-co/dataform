import { dataform } from "df/protos/ts";

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
