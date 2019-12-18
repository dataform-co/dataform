import { adapters } from "@dataform/core";
import { Assertion } from "@dataform/core/assertion";
import { Declaration } from "@dataform/core/declaration";
import { Operation } from "@dataform/core/operation";
import { IActionProto, Resolvable, Session } from "@dataform/core/session";
import {
  DistStyleTypes,
  ignoredProps,
  SortStyleTypes,
  Table,
  TableTypes
} from "@dataform/core/table";
import { dataform } from "@dataform/protos";

const SQL_DATA_WAREHOUSE_DIST_HASH_REGEXP = new RegExp("HASH\\s*\\(\\s*\\w*\\s*\\)\\s*");

function relativePath(path: string, base: string) {
  if (base.length === 0) {
    return path;
  }
  const stripped = path.substr(base.length);
  if (stripped.startsWith("/")) {
    return stripped.substr(1);
  } else {
    return stripped;
  }
}

export function baseFilename(path: string) {
  const pathSplits = path.split("/");
  return pathSplits[pathSplits.length - 1].split(".")[0];
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
    lastfile = stack.shift().getFileName();
    if (!lastfile) {
      continue;
    }
    if (!lastfile.includes(rootDir)) {
      continue;
    }
    if (lastfile.includes("node_modules")) {
      continue;
    }
    if (!(lastfile.includes("definitions/") || lastfile.includes("models/"))) {
      continue;
    }
    break;
  }
  return relativePath(lastfile, rootDir);
}

function getCurrentStack(): NodeJS.CallSite[] {
  const originalPrepareStackTrace = Error.prepareStackTrace;
  try {
    Error.prepareStackTrace = (err, stack) => {
      return stack;
    };
    return (new Error().stack as unknown) as NodeJS.CallSite[];
  } finally {
    Error.prepareStackTrace = originalPrepareStackTrace;
  }
}

export function graphHasErrors(graph: dataform.ICompiledGraph) {
  const graphErrors = validate(graph);

  return (
    (graphErrors.compilationErrors && graphErrors.compilationErrors.length > 0) ||
    (graphErrors.validationErrors && graphErrors.validationErrors.length > 0)
  );
}

function joinQuoted(values: string[]) {
  return values.map((value: string) => `"${value}"`).join(" | ");
}

function objectExistsOrIsNonEmpty(prop: any): boolean {
  if (!prop) {
    return false;
  }

  return (
    (Array.isArray(prop) && !!prop.length) ||
    (!Array.isArray(prop) && typeof prop === "object" && !!Object.keys(prop).length) ||
    typeof prop !== "object"
  );
}

export function validate(compiledGraph: dataform.ICompiledGraph): dataform.IGraphErrors {
  const validationErrors: dataform.IValidationError[] = [];

  // Table validation
  compiledGraph.tables.forEach(action => {
    const actionName = action.name;

    // type
    if (!!action.type && !Object.values(TableTypes).includes(action.type)) {
      const predefinedTypes = joinQuoted(Object.values(TableTypes));
      const message = `Wrong type of table detected. Should only use predefined types: ${predefinedTypes}`;
      validationErrors.push(dataform.ValidationError.create({ message, actionName }));
    }

    // sqldatawarehouse config
    if (action.sqlDataWarehouse && action.sqlDataWarehouse.distribution) {
      const distribution = action.sqlDataWarehouse.distribution.toUpperCase();

      if (
        distribution !== "REPLICATE" &&
        distribution !== "ROUND_ROBIN" &&
        !SQL_DATA_WAREHOUSE_DIST_HASH_REGEXP.test(distribution)
      ) {
        const message = `Invalid value for sqldatawarehouse distribution: "${distribution}"`;
        validationErrors.push(dataform.ValidationError.create({ message, actionName }));
      }
    }

    // redshift config
    if (!!action.redshift) {
      const validatePropertyDefined = (
        opts: dataform.IRedshiftOptions,
        prop: keyof dataform.IRedshiftOptions
      ) => {
        const error = dataform.ValidationError.create({
          message: `Property "${prop}" is not defined`,
          actionName
        });
        const value = opts[prop];
        if (!opts.hasOwnProperty(prop)) {
          validationErrors.push(error);
        } else if (value instanceof Array) {
          if (value.length === 0) {
            validationErrors.push(error);
          }
        }
      };
      const validatePropertiesDefined = (
        opts: dataform.IRedshiftOptions,
        props: Array<keyof dataform.IRedshiftOptions>
      ) => props.forEach(prop => validatePropertyDefined(opts, prop));
      const validatePropertyValueInValues = (
        opts: dataform.IRedshiftOptions,
        prop: keyof dataform.IRedshiftOptions & ("distStyle" | "sortStyle"),
        values: string[]
      ) => {
        if (!!opts[prop] && !values.includes(opts[prop])) {
          const message = `Wrong value of "${prop}" property. Should only use predefined values: ${joinQuoted(
            values
          )}`;
          validationErrors.push(dataform.ValidationError.create({ message, actionName }));
        }
      };

      if (action.redshift.distStyle || action.redshift.distKey) {
        validatePropertiesDefined(action.redshift, ["distStyle", "distKey"]);
        validatePropertyValueInValues(action.redshift, "distStyle", Object.values(DistStyleTypes));
      }
      if (
        action.redshift.sortStyle ||
        (action.redshift.sortKeys && action.redshift.sortKeys.length)
      ) {
        validatePropertiesDefined(action.redshift, ["sortStyle", "sortKeys"]);
        validatePropertyValueInValues(action.redshift, "sortStyle", Object.values(SortStyleTypes));
      }
    }

    // BigQuery config
    if (!!action.bigquery) {
      if (action.bigquery.partitionBy && action.type === "view") {
        const error = dataform.ValidationError.create({
          message: `partitionBy is not valid for BigQuery views; it is only valid for tables`,
          actionName
        });
        validationErrors.push(error);
      }
    }

    // ignored properties in tables
    if (!!ignoredProps[action.type]) {
      ignoredProps[action.type].forEach(ignoredProp => {
        if (objectExistsOrIsNonEmpty(action[ignoredProp])) {
          const message = `Unused property was detected: "${ignoredProp}". This property is not used for tables with type "${action.type}" and will be ignored.`;
          validationErrors.push(dataform.ValidationError.create({ message, actionName }));
        }
      });
    }
  });

  const compilationErrors =
    compiledGraph.graphErrors && compiledGraph.graphErrors.compilationErrors
      ? compiledGraph.graphErrors.compilationErrors
      : [];

  return dataform.GraphErrors.create({ validationErrors, compilationErrors });
}

export function flatten<T>(nestedArray: T[][]) {
  return nestedArray.reduce((previousValue: T[], currentValue: T[]) => {
    return previousValue.concat(currentValue);
  }, []);
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
  name: string,
  schema: string,
  database?: string
): dataform.ITarget {
  return dataform.Target.create({
    name: adapter.normalizeIdentifier(name),
    schema: adapter.normalizeIdentifier(schema),
    database: database && adapter.normalizeIdentifier(database)
  });
}

export function setNameAndTarget(
  session: Session,
  action: IActionProto,
  name: string,
  overrideSchema?: string,
  overrideDatabase?: string
) {
  action.target = target(
    session.adapter(),
    name,
    overrideSchema || session.config.defaultSchema,
    overrideDatabase || session.config.defaultDatabase
  );
  const nameParts = [action.target.name, action.target.schema];
  if (!!action.target.database) {
    nameParts.push(action.target.database);
  }
  action.name = nameParts.reverse().join(".");
}
