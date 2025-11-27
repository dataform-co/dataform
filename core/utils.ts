import { Action, ActionProto } from "df/core/actions";
import { Assertion } from "df/core/actions/assertion";
import { DataPreparation } from "df/core/actions/data_preparation";
import { IncrementalTable } from "df/core/actions/incremental_table";
import { Notebook } from "df/core/actions/notebook";
import { Operation } from "df/core/actions/operation";
import { Table } from "df/core/actions/table";
import { View } from "df/core/actions/view";
import { Contextable, Resolvable } from "df/core/contextables";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
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

export function resolvableAsTarget(
  resolvable: Resolvable | dataform.ActionConfig.Target
): dataform.Target {
  if (typeof resolvable === "string") {
    return dataform.Target.create({
      name: resolvable
    });
  }
  const actionConfigTarget = (resolvable as dataform.ActionConfig.ITarget);
  if (actionConfigTarget instanceof dataform.ActionConfig.Target || actionConfigTarget.dataset !== undefined || actionConfigTarget.project !== undefined) {
    return dataform.Target.create({
      name: actionConfigTarget.name,
      schema: actionConfigTarget.dataset,
      database: actionConfigTarget.project,
      includeDependentAssertions: actionConfigTarget.includeDependentAssertions,
    });
  }
  return dataform.Target.create(resolvable);
}

export function resolvableAsActionConfigTarget(
  resolvable: string | object
): dataform.ActionConfig.ITarget {
  if (typeof resolvable === "string") {
    const parts = resolvable.split(".").reverse();
    if (!isResolvableArray(parts)) {
      throw new Error(invalidRefInputMessage);
    }

    const [name, schema, database] = parts;
    return {
      name,
      dataset: schema,
      project: database,
    };
  }

  return resolvable as dataform.ActionConfig.ITarget;
}

export function stringifyResolvable(res: Resolvable) {
  return typeof res === "string" ? res : JSON.stringify(res);
}

export function ambiguousActionNameMsg(act: Resolvable, allActs: Action[] | string[]) {
  const allActNames =
    typeof allActs[0] === "string"
      ? allActs
      : (allActs as Array<Table | Operation | Assertion>).map(
        r => `${r.getTarget().schema}.${r.getTarget().name}`
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
        `Unexpected property "${extraProperties[0]}"${!!name ? ` in ${name}` : ""
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

export function validateNoMixedCompilationMode(
  session: Session,
  filename: string,
  contextableQuery: Contextable<any, any>,
  contextableWhere: Contextable<any, any>,
  contextablePostOps: Array<Contextable<any, any>>,
  contextablePreOps: Array<Contextable<any, any>>
) {
  let flattenPostOps: unknown[] = [];
  contextablePostOps.forEach(op => {
    flattenPostOps = flattenPostOps.concat(typeof op === "object" ? op : [op]);
  });
  let flattenPreOps: unknown[] = [];
  contextablePreOps.forEach(op => {
    flattenPreOps = flattenPreOps.concat(typeof op === "object" ? op : [op]);
  });
  const conflictingProperties: string[] = [];
  if (!!contextableQuery) { conflictingProperties.push("query"); }
  if (!!contextableWhere) { conflictingProperties.push("where"); }
  if (!!flattenPostOps.length) { conflictingProperties.push("postOps"); }
  if (!!flattenPreOps.length) { conflictingProperties.push("preOps"); }
  if (conflictingProperties.length) {
    const err = new Error(
      `Cannot mix AoT and JiT compilation in action. The following AoT properties were found: ${conflictingProperties.join(", ")
      }`
    );
    session.compileError(err, filename);
    throw err;
  }
}

/**
 * Checks if the Cloud Resource connection has a valid format.
 * @param connection String to be validated.
 */
export function validateConnectionFormat(
  connection: string,
) {
  // Connection pattern of the form project.location.connection_id. Example:
  // my-project.us-central1.my-connection
  const dotPattern = /^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$/;

  // Connection pattern of the form projects/<substring>/locations/<substring>/connections/<substring>
  // Example: projects/my-project/locations/us-central1/connections/my-connection
  // Substrings cannot contain '/'.
  const resourcePattern =
    /^projects\/[^/]+\/locations\/[^/]+\/connections\/[^/]+$/;

  const isValidFormat =
    dotPattern.test(connection) || resourcePattern.test(connection);

  if (connection !== 'DEFAULT' && !isValidFormat) {
    throw new Error(
      'The connection must be in the format `{project}.{location}.{connection_id}` or `projects/{project}/locations/{location}/connections/{connection_id}`, or be set to `DEFAULT`.',
    );
  }

}

/**
 * Checks if the storageUri is a valid GCS path.
 * @param storageUri String to be validated.
 */
export function validateStorageUriFormat(
  storageUri: string,
) {
  // storageUri must have format gs://<bucket_name>/<path_to_data>
  const gcsPathPattern = /^gs:\/\/([^/]+)\/(.+)$/;

  if (!gcsPathPattern.test(storageUri)) {
    throw new Error(
      'The storage URI must be in the format `gs://{bucket_name}/{path_to_data}`.',
    );
  }
}

/**
 * Returns a file format for an Iceberg table, as specified in the user's config file.
 * @param configFileFormat User-provided file format, if it exists.
 * @return File format used when creating an Iceberg table.
 */
export function getFileFormatValueForIcebergTable(
  configFileFormat?: string,
): dataform.FileFormat {
  if (!configFileFormat) {
    // Default to PARQUET if fileFormat is undefined.
    return dataform.FileFormat.PARQUET;
  }

  switch (configFileFormat.toUpperCase()) {
    case "PARQUET":
      return dataform.FileFormat.PARQUET;

    default:
      throw new Error(
        `File format ${configFileFormat} is not supported.`,
      );
  }
}

/**
 * Returns the connection for an Iceberg table, as specified in the user's config file.
 * Defaults to "DEFAULT" if no connection is provided.
 * @param configConnection defined in the config block
 * @param defaultConnection defined in workflow_settings.yaml.
 * @returns Connection used when creating an Iceberg table.
 */
export function getConnectionForIcebergTable(
  configConnection?: string,
  defaultConnection?: string,
): string {
  if (configConnection) {
    return configConnection;
  } else if (defaultConnection) {
    return defaultConnection;
  } else {
    return "DEFAULT";
  }
}

/**
 * Constructs the storage URI for an Iceberg table from storageUri, bucketName,
 * tableFolderRoot, and tableFolderSubpath provided in the config file. Returns
 * undefined if a complete URI cannot be formed.
 * @returns Storage URI used when creating an Iceberg table.
 */
export function getStorageUriForIcebergTable(
  bucketName: string,
  tableFolderRoot: string,
  tableFolderSubpath: string,
): string {
  return `gs://${bucketName}/${tableFolderRoot}/${tableFolderSubpath}`;
}

/**
 * Returns the bucketName which will be used to construct storageUri for an
 * Iceberg table. If the bucketName is provided in the config block, that value
 * will be used. Otherwise, defaultBucketName defined in workflow_settings.yaml
 * will be used. If none of those two values are defined, we throw an error.
 * @param defaultBucketName defined in workflow_settings.yaml
 * @param configBucketName defined in the config block
 * @returns bucketName used to construct storageUri for Iceberg tables
 */
export function getEffectiveBucketName(
  defaultBucketName?: string,
  configBucketName?: string,
): string {
  if (configBucketName) {
    return configBucketName;
  } else if (defaultBucketName) {
    return defaultBucketName;
  } else {
    throw new Error(
      "When defining an Iceberg table, bucket name must be defined in workflow_settings.yaml or the config block."
    );
  }
}

/**
 * Returns the tableFolderRoot which will be used to construct storageUri for an
 * Iceberg table. If the tableFolderRoot is provided in the config block, that
 * value will be used. Otherwise, defaultTableFolderRoot defined in
 * workflow_settings.yaml will be used. If none of those two values are
 * defined, "_dataform" will be used.
 * @param defaultTableFolderRoot defined in workflow_settings.yaml
 * @param configTableFolderRoot defined in the config block
 * @returns tableFolderRoot used to construct storageUri for Iceberg tables
 */
export function getEffectiveTableFolderRoot(
  defaultTableFolderRoot?: string,
  configTableFolderRoot?: string,
): string {
  if (configTableFolderRoot) {
    return configTableFolderRoot;
  } else if (defaultTableFolderRoot) {
    return defaultTableFolderRoot;
  } else {
    return "_dataform";
  }
}

/**
 * Returns the tableFolderSubpath which will be used to construct storageUri for
 * an Iceberg table. If the tableFolderSubpath is provided in the config block,
 * that value will be used. Otherwise, defaultTableFolderSubpath defined in
 * workflow_settings.yaml will be used. If none of those two values are
 * defined, "{dataset_name}/{table_name}"" will be used.
 * @param datasetName Might be used to construct the tableFolderSubpath if no alternative value is available.
 * @param tableName Might be used to construct the tableFolderSubpath if no alternative value is available.
 * @param defaultTableFolderSubpath defined in workflow_settings.yaml
 * @param configTableFolderSubpath defined in the config block
 */
export function getEffectiveTableFolderSubpath(
  datasetName: string,
  tableName: string,
  defaultTableFolderSubpath?: string,
  configTableFolderSubpath?: string,
): string {
  if (configTableFolderSubpath) {
    return configTableFolderSubpath;
  } else if (defaultTableFolderSubpath) {
    return defaultTableFolderSubpath;
  } else {
    return `${datasetName}/${tableName}`;
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
    | dataform.ActionConfig.DataPreparationConfig.ErrorTableConfig
    | dataform.ActionConfig.Target
): dataform.Target {
  const compiledGraphTarget = dataform.Target.create({ name: actionConfig.name });
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
  return compiledGraphTarget;
}

export function resolveActionsConfigFilename(configFilename: string, configPath: string) {
  return Path.normalize(Path.join(Path.dirName(configPath), configFilename));
}

export function checkAssertionsForDependency(
  action: actionsWithDependencies,
  resolvable: Resolvable
): dataform.Target {
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
        action.getFileName(),
        action.getTarget()
      );
      return;
    }
  }
  action.includeAssertionsForDependency.set(
    dependencyTargetString,
    dependencyTarget.includeDependentAssertions
  );
  return dependencyTarget;
}

/** 
 * Multimap of resolvable targets to values. 
 * Allows the lookup by name/schema/database and their subsets.
 */
export class ResolvableMap<T> {
  private byName: Map<string, T[]> = new Map();
  private bySchemaAndName: Map<string, Map<string, T[]>> = new Map();
  private byDatabaseAndName: Map<string, Map<string, T[]>> = new Map();
  private byDatabaseSchemaAndName: Map<string, Map<string, Map<string, T[]>>> = new Map();

  public constructor(values?: Array<{ actionTarget: dataform.ITarget, value: T }>) {
    if (values) {
      for (const { actionTarget, value } of values) {
        this.set(actionTarget, value);
      }
    }
  }

  public set(actionTarget: dataform.ITarget, value: T) {
    this.setByNameLevel(this.byName, actionTarget.name, value);

    if (!!actionTarget.schema) {
      this.setBySchemaLevel(this.bySchemaAndName, actionTarget, value);
    }

    if (!!actionTarget.database) {
      if (!this.byDatabaseAndName.has(actionTarget.database)) {
        this.byDatabaseAndName.set(actionTarget.database, new Map());
      }
      const forDatabaseNoSchema = this.byDatabaseAndName.get(actionTarget.database);
      this.setByNameLevel(forDatabaseNoSchema, actionTarget.name, value);

      if (!!actionTarget.schema) {
        if (!this.byDatabaseSchemaAndName.has(actionTarget.database)) {
          this.byDatabaseSchemaAndName.set(actionTarget.database, new Map());
        }
        const forDatabase = this.byDatabaseSchemaAndName.get(actionTarget.database);
        this.setBySchemaLevel(forDatabase, actionTarget, value);
      }
    }
  }

  public find(actionTarget: dataform.ITarget): T[] {
    if (!!actionTarget.database) {
      if (!!actionTarget.schema) {
        return (
          this.byDatabaseSchemaAndName
            .get(actionTarget.database)
            ?.get(actionTarget.schema)
            ?.get(actionTarget.name) || []
        );
      }
      return this.byDatabaseAndName.get(actionTarget.database)?.get(actionTarget.name) || [];
    }
    if (!!actionTarget.schema) {
      return this.bySchemaAndName.get(actionTarget.schema)?.get(actionTarget.name) || [];
    }
    return this.byName.get(actionTarget.name) || [];
  }

  private setByNameLevel(targetMap: Map<string, T[]>, name: string, value: T) {
    if (!targetMap.has(name)) {
      targetMap.set(name, []);
    }
    targetMap.get(name).push(value);
  }

  private setBySchemaLevel(
    targetMap: Map<string, Map<string, T[]>>,
    actionTarget: dataform.ITarget,
    value: T
  ) {
    if (!targetMap.has(actionTarget.schema)) {
      targetMap.set(actionTarget.schema, new Map());
    }
    const forSchema = targetMap.get(actionTarget.schema);
    this.setByNameLevel(forSchema, actionTarget.name, value);
  }
}
