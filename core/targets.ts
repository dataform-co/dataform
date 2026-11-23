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

export const invalidRefInputMessage =
  "Invalid input. Accepted inputs include: a single object containing " +
  "an (optional) 'database', (optional) 'schema' (or 'catalog' and 'namespace'), and 'name', " +
  "or 1-4 inputs consisting of an (optional) database, (optional) catalog and namespace (or schema), and 'name'.";

export function isResolvableArray(parts: any[]): parts is [string, string?, string?, string?] {
  if (parts.some((part) => typeof part !== "string")) {
    return false;
  }
  return parts.length > 0 && parts.length <= 4;
}

export function resolvableAsActionConfigTarget(
  resolvable: string | object,
): dataform.ActionConfig.ITarget {
  if (typeof resolvable === "string") {
    const parts = resolvable.split(".").reverse();
    if (!isResolvableArray(parts) || parts.length > 3) {
      throw new Error(invalidRefInputMessage);
    }

    const [name, schema, database] = parts;
    return {
      name,
      dataset: schema,
      project: database,
    };
  }

  const obj = resolvable as any;
  if (obj && typeof obj === "object") {
    const catalog = obj.catalog ?? obj.lakehouse?.catalog;
    const namespace = obj.namespace ?? obj.lakehouse?.namespace;
    if (catalog != null || namespace != null || obj.lakehouse != null) {
      return {
        name: obj.name,
        project: obj.project ?? obj.database,
        lakehouse: {
          catalog,
          namespace,
        },
        includeDependentAssertions: obj.includeDependentAssertions,
      };
    }
  }

  return resolvable as dataform.ActionConfig.ITarget;
}
