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
 * and it should not be used for indexing. Use {@code targetStringifier} instead.
 *
 * TODO(ekrekr): replace remaining incorrect usage of this with targetStringifier.
 */
export function targetAsReadableString(target: dataform.ITarget): string {
  const nameParts = [target.name, target.schema];
  if (!!target.database) {
    nameParts.push(target.database);
  }
  return nameParts.reverse().join(".");
}
