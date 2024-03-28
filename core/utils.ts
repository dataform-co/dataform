import { Action } from "df/core/actions";
import { Assertion } from "df/core/actions/assertion";
import { Operation } from "df/core/actions/operation";
import { Table } from "df/core/actions/table";
import { Resolvable } from "df/core/common";
import * as Path from "df/core/path";
import { IActionProto, Session } from "df/core/session";
import { dataform } from "df/protos/ts";

declare var __webpack_require__: any;
declare var __non_webpack_require__: any;

// This side-steps webpack's require in favour of the real require.
export const nativeRequire =
  typeof __webpack_require__ === "function" ? __non_webpack_require__ : require;

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
        nextLastfile.includes(`definitions${Path.separator}`) ||
        nextLastfile.includes(`models${Path.separator}`)
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
  return Path.relativePath(lastfile, rootDir);
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

export function ambiguousActionNameMsg(act: Resolvable, allActs: Action[] | string[]) {
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
  config: dataform.IProjectConfig,
  name: string,
  schema?: string,
  database?: string
): dataform.ITarget {
  return dataform.Target.create({
    name,
    schema: schema || config.defaultSchema || undefined,
    database: database || config.defaultDatabase || undefined
  });
}

export function setNameAndTarget(
  session: Session,
  action: IActionProto,
  name: string,
  overrideSchema?: string,
  overrideDatabase?: string
) {
  action.target = target(session.config, name, overrideSchema, overrideDatabase);
  action.canonicalTarget = target(session.canonicalConfig, name, overrideSchema, overrideDatabase);
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

export function tableTypeStringToEnum(type: string, throwIfUnknown: boolean) {
  switch (type) {
    case "table":
      return dataform.TableType.TABLE;
    case "incremental":
      return dataform.TableType.INCREMENTAL;
    case "view":
      return dataform.TableType.VIEW;
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

export function extractActionDetailsFromFileName(
  path: string
): { fileExtension: string; fileNameAsTargetName: string } {
  const fileName = Path.fileName(path);
  const fileExtension = Path.fileExtension(path);
  return { fileExtension, fileNameAsTargetName: fileName };
}

export function actionConfigToCompiledGraphTarget(
  // The target interface is used here because Action configs contain all the fields of action
  // config targets, even if they are not strictly target objects.
  actionConfigTarget: dataform.ActionConfig.ITarget
): dataform.Target {
  const compiledGraphTarget: dataform.ITarget = { name: actionConfigTarget.name };
  if (actionConfigTarget.dataset) {
    compiledGraphTarget.schema = actionConfigTarget.dataset;
  }
  if (actionConfigTarget.project) {
    compiledGraphTarget.database = actionConfigTarget.project;
  }
  return dataform.Target.create(compiledGraphTarget);
}
