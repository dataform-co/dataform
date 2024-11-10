import { Action } from "df/core/actions";
import { Assertion } from "df/core/actions/assertion";
import { DataPreparation } from "df/core/actions/data_preparation";
import { IncrementalTable } from "df/core/actions/incremental_table";
import { Notebook } from "df/core/actions/notebook";
import { Operation } from "df/core/actions/operation";
import { Table } from "df/core/actions/table";
import { View } from "df/core/actions/view";
import { Resolvable } from "df/core/common";
import * as Path from "df/core/path";
import { IActionProto, Session } from "df/core/session";
import { dataform } from "df/protos/ts";

declare var __webpack_require__: any;
declare var __non_webpack_require__: any;

type actionsWithDependencies =
  | Table
  | View
  | IncrementalTable
  | Operation
  | Notebook
  | DataPreparation;

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

/**
 * @deprecated use ActionBuilder.applySessionToTarget() instead.
 */
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

/**
 * @deprecated use ActionBuilder.applySessionToTarget() instead.
 */
export function setNameAndTarget(
  session: Session,
  action: IActionProto,
  name: string,
  overrideSchema?: string,
  overrideDatabase?: string
) {
  action.target = target(session.projectConfig, name, overrideSchema, overrideDatabase);
  action.canonicalTarget = target(
    session.canonicalProjectConfig,
    name,
    overrideSchema,
    overrideDatabase
  );
  if (action.target.name.includes(".")) {
    session.compileError(
      new Error("Action target names cannot include '.'"),
      undefined,
      action.target
    );
  }
  if (action.target.schema.includes(".")) {
    session.compileError(
      new Error("Action target datasets cannot include '.'"),
      undefined,
      action.target
    );
  }
  if (action.target.database.includes(".")) {
    session.compileError(
      new Error("Action target projects cannot include '.'"),
      undefined,
      action.target
    );
  }
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
 * @deprecated verifyObjectMatchesProto will be replacing this soon.
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
  const basename = Path.basename(path);
  const fileExtension = Path.fileExtension(path);
  return { fileExtension, fileNameAsTargetName: basename };
}

// Converts the config proto's target proto to the compiled graph proto's representation.
export function configTargetToCompiledGraphTarget(configTarget: dataform.ActionConfig.Target) {
  const compiledGraphTarget: dataform.ITarget = { name: configTarget.name };
  if (configTarget.project) {
    compiledGraphTarget.database = configTarget.project;
  }
  if (configTarget.dataset) {
    compiledGraphTarget.schema = configTarget.dataset;
  }
  if (configTarget.hasOwnProperty("includeDependentAssertions")) {
    compiledGraphTarget.includeDependentAssertions = configTarget.includeDependentAssertions;
  }
  return dataform.Target.create(compiledGraphTarget);
}

// Converts a config proto's action config proto to the compiled graph proto's representation.
// Action config protos roughly contain target protos fields.
export function actionConfigToCompiledGraphTarget(
  actionConfig:
    | dataform.ActionConfig.TableConfig
    | dataform.ActionConfig.ViewConfig
    | dataform.ActionConfig.IncrementalTableConfig
    | dataform.ActionConfig.OperationConfig
    | dataform.ActionConfig.AssertionConfig
    | dataform.ActionConfig.DeclarationConfig
    | dataform.ActionConfig.NotebookConfig
    | dataform.ActionConfig.DataPreparationConfig
): dataform.Target {
  const compiledGraphTarget: dataform.ITarget = { name: actionConfig.name };
  if ("project" in actionConfig && actionConfig.project !== undefined) {
    compiledGraphTarget.database = actionConfig.project;
  }
  if ("location" in actionConfig && actionConfig.location !== undefined) {
    // This is a hack around the limitations of the compiled graph's target proto not having a
    // "location" field.
    compiledGraphTarget.schema = actionConfig.location;
  }
  if ("dataset" in actionConfig && actionConfig.dataset !== undefined) {
    compiledGraphTarget.schema = actionConfig.dataset;
  }
  return dataform.Target.create(compiledGraphTarget);
}

export function resolveActionsConfigFilename(configFilename: string, configPath: string) {
  return Path.normalize(Path.join(Path.dirName(configPath), configFilename));
}

export function addDependenciesToActionDependencyTargets(
  action: actionsWithDependencies,
  resolvable: Resolvable
) {
  const dependencyTarget = resolvableAsTarget(resolvable);
  if (
    !dependencyTarget.hasOwnProperty("includeDependentAssertions") &&
    !(action instanceof DataPreparation)
  ) {
    // dependency `includeDependentAssertions` takes precedence over the config's `dependOnDependencyAssertions`
    dependencyTarget.includeDependentAssertions = action.dependOnDependencyAssertions;
  }

  // check if same dependency already exist in this action but with opposite value for includeDependentAssertions
  const dependencyTargetString = action.session.compilationSql().resolveTarget(dependencyTarget);

  if (action.includeAssertionsForDependency.has(dependencyTargetString)) {
    if (
      action.includeAssertionsForDependency.get(dependencyTargetString) !==
      dependencyTarget.includeDependentAssertions
    ) {
      action.session.compileError(
        `Conflicting "includeDependentAssertions" properties are not allowed. Dependency ${dependencyTarget.name} has different values set for this property.`,
        action.proto.fileName,
        action.proto.target
      );
      return action;
    }
  }
  action.proto.dependencyTargets.push(dependencyTarget);
  action.includeAssertionsForDependency.set(
    dependencyTargetString,
    dependencyTarget.includeDependentAssertions
  );
}
