import { adapters } from "df/core";
import { Assertion } from "df/core/assertion";
import { Resolvable } from "df/core/common";
import { Declaration } from "df/core/declaration";
import { Operation } from "df/core/operation";
import { IActionProto, Session } from "df/core/session";
import { Table } from "df/core/table";
import { dataform } from "df/protos/ts";

export const pathSeperator = (() => {
  if (typeof process !== "undefined") {
    return process.platform === "win32" ? "\\" : "/";
  }
  return "/";
})();

function relativePath(fullPath: string, base: string) {
  if (base.length === 0) {
    return fullPath;
  }
  const stripped = fullPath.substr(base.length);
  if (stripped.startsWith(pathSeperator)) {
    return stripped.substr(1);
  } else {
    return stripped;
  }
}

export function baseFilename(fullPath: string) {
  return fullPath
    .split(pathSeperator)
    .slice(-1)[0]
    .split(".")[0];
}

export function matchPatterns(patterns: string[], values: string[]) {
  const fullyQualifiedActions: string[] = [];
  patterns.forEach(pattern => {
    if (pattern.includes(".")) {
      if (values.includes(pattern)) {
        fullyQualifiedActions.push(pattern);
      }
    } else {
      const matchingActions = values.filter(value => pattern === value.split(".").slice(-1)[0]);
      if (matchingActions.length === 0) {
        return;
      }
      if (matchingActions.length > 1) {
        throw new Error(ambiguousActionNameMsg(pattern, matchingActions));
      }
      fullyQualifiedActions.push(matchingActions[0]);
    }
  });
  return fullyQualifiedActions;
}

export function getCallerFile(rootDir: string) {
  let lastfile: string;
  const stack = getCurrentStack();
  while (stack.length) {
    const nextLastfile = stack.shift().getFileName();
    if (!nextLastfile) {
      continue;
    }
    if (!nextLastfile.includes(rootDir)) {
      continue;
    }
    if (nextLastfile.includes("node_modules")) {
      continue;
    }
    // If it's in the root directory we'll take it, but keep searching
    // for a better match.
    lastfile = nextLastfile;
    if (
      !(
        nextLastfile.includes(`definitions${pathSeperator}`) ||
        nextLastfile.includes(`models${pathSeperator}`)
      )
    ) {
      continue;
    }
    break;
  }
  if (!lastfile) {
    // This is likely caused by Session.compileError() being called inside Session.compile().
    // If so, explicitly pass the filename to Session.compileError().
    throw new Error("Unable to find valid caller file; please report this issue.");
  }
  return relativePath(lastfile, rootDir);
}

function getCurrentStack(): NodeJS.CallSite[] {
  const originalStackTraceLimit = Error.stackTraceLimit;
  const originalPrepareStackTrace = Error.prepareStackTrace;
  try {
    Error.stackTraceLimit = Number.POSITIVE_INFINITY;
    Error.prepareStackTrace = (err, stack) => {
      return stack;
    };
    return (new Error().stack as unknown) as NodeJS.CallSite[];
  } finally {
    Error.stackTraceLimit = originalStackTraceLimit;
    Error.prepareStackTrace = originalPrepareStackTrace;
  }
}

export function graphHasErrors(graph: dataform.ICompiledGraph) {
  return graph.graphErrors?.compilationErrors.length > 0;
}

const invalidRefInputMessage =
  "Invalid input. Accepted inputs include: a single object containing " +
  "an (optional) 'database', (optional) 'schema', and 'name', " +
  "or 1-3 inputs consisting of an (optional) database, (optional) schema, and 'name'.";

export function toResolvable(ref: Resolvable | string[], rest: string[] = []): Resolvable {
  if (Array.isArray(ref) && rest.length > 0) {
    throw new Error(invalidRefInputMessage);
  }
  if (rest.length === 0 && !Array.isArray(ref)) {
    return ref;
  }
  const resolvableArray = Array.isArray(ref) ? ref.reverse() : [ref, ...rest].reverse();
  if (!isResolvableArray(resolvableArray)) {
    throw new Error(invalidRefInputMessage);
  }
  const [name, schema, database] = resolvableArray;
  return { database, schema, name };
}

function isResolvableArray(parts: any[]): parts is [string, string?, string?] {
  if (parts.some(part => typeof part !== "string")) {
    return false;
  }
  return parts.length > 0 && parts.length <= 3;
}

export function resolvableAsTarget(resolvable: Resolvable): dataform.ITarget {
  if (typeof resolvable === "string") {
    return {
      name: resolvable
    };
  }
  return resolvable;
}

export function stringifyResolvable(res: Resolvable) {
  return typeof res === "string" ? res : JSON.stringify(res);
}

export function ambiguousActionNameMsg(
  act: Resolvable,
  allActs: Array<Table | Operation | Assertion | Declaration> | string[]
) {
  const allActNames =
    typeof allActs[0] === "string"
      ? allActs
      : (allActs as Array<Table | Operation | Assertion>).map(
          r => `${r.proto.target.schema}.${r.proto.target.name}`
        );
  return `Ambiguous Action name: ${stringifyResolvable(
    act
  )}. Did you mean one of: ${allActNames.join(", ")}.`;
}

export function target(
  adapter: adapters.IAdapter,
  config: dataform.IProjectConfig,
  name: string,
  schema?: string,
  database?: string
): dataform.ITarget {
  schema = schema || config.defaultSchema;
  database = database || config.defaultDatabase;
  return dataform.Target.create({
    name: adapter.normalizeIdentifier(name),
    schema: !!schema ? adapter.normalizeIdentifier(schema || config.defaultSchema) : undefined,
    database: !!database ? adapter.normalizeIdentifier(database) : undefined
  });
}

export function setNameAndTarget(
  session: Session,
  action: IActionProto,
  name: string,
  overrideSchema?: string,
  overrideDatabase?: string
) {
  action.target = target(session.adapter(), session.config, name, overrideSchema, overrideDatabase);
  action.canonicalTarget = target(
    session.adapter(),
    session.canonicalConfig,
    name,
    overrideSchema,
    overrideDatabase
  );
}

/**
 * Checks that the given list of keys completely covers all supported keys in the given interface.
 */
export function strictKeysOf<T>() {
  return <U extends Array<keyof T>>(
    array: U & ([keyof T] extends [U[number]] ? unknown : Array<["Needs to be all of", T]>)
  ) => array;
}

/**
 * Will throw an error if the provided object contains any properties that aren't in the provided list.
 */
export function checkExcessProperties<T>(
  reportError: (e: Error) => void,
  object: T,
  supportedProperties: string[],
  name?: string
) {
  const extraProperties = Object.keys(object).filter(
    key => !(supportedProperties as string[]).includes(key)
  );
  if (extraProperties.length > 0) {
    reportError(
      new Error(
        `Unexpected property "${extraProperties[0]}"${
          !!name ? ` in ${name}` : ""
        }. Supported properties are: ${JSON.stringify(supportedProperties)}`
      )
    );
  }
}

export function validateQueryString(session: Session, query: string, filename: string) {
  if (query?.trim().slice(-1) === ";") {
    session.compileError(
      new Error(
        "Semi-colons are not allowed at the end of SQL statements."
        // This can break the statement because of appended adapter specific SQL.
      ),
      filename
    );
  }
}

export function throwIfInvalid<T>(proto: T, verify: (proto: T) => string) {
  const verifyError = verify(proto);
  if (verifyError) {
    throw new Error(verifyError);
  }
}

export function tableTypeStringToEnum(type: string, throwIfUnknown: boolean) {
  switch (type) {
    case "table":
      return dataform.TableType.TABLE;
    case "incremental":
      return dataform.TableType.INCREMENTAL;
    case "view":
      return dataform.TableType.VIEW;
    case "inline":
      return dataform.TableType.INLINE;
    default: {
      if (throwIfUnknown) {
        throw new Error(`Unexpected table type: ${type}`);
      }
      return dataform.TableType.UNKNOWN_TYPE;
    }
  }
}

export function tableTypeEnumToString(enumType: dataform.TableType) {
  return dataform.TableType[enumType].toLowerCase();
}

export function setOrValidateTableEnumType(table: dataform.ITable) {
  let enumTypeFromStr: dataform.TableType | null = null;
  if (table.type !== "" && table.type !== undefined) {
    enumTypeFromStr = tableTypeStringToEnum(table.type, true);
  }
  if (table.enumType === dataform.TableType.UNKNOWN_TYPE || table.enumType === undefined) {
    table.enumType = enumTypeFromStr!;
  } else if (enumTypeFromStr !== null && table.enumType !== enumTypeFromStr) {
    throw new Error(
      `Table str type "${table.type}" and enumType "${tableTypeEnumToString(
        table.enumType
      )}" are not equivalent.`
    );
  }
}
