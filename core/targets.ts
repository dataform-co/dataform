import { JSONObjectStringifier } from "df/common/strings/stringifier";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

/**
 * Produces an unambigous mapping to and from a string representation.
 */
export const targetStringifier = JSONObjectStringifier.create<core.Target>();

/**
 * Returns true if both targets are equal.
 */
export function targetsAreEqual(a: core.Target, b: core.Target) {
  return a.database === b.database && a.schema === b.schema && a.name === b.name;
}

/**
 * Provides a readable string representation of the target which is used for e.g. specifying
 * actions on the CLI.
 * This is effectively equivelant to an action "name".
 *
 * This is an ambiguous transformation, multiple targets may map to the same string
 * and it should not be used for indexing. Use {@code targetStringifier} instead.
 */
export function targetAsReadableString(target: core.Target): string {
  const nameParts = [target.name, target.schema];
  if (!!target.database) {
    nameParts.push(target.database);
  }
  return nameParts.reverse().join(".");
}
