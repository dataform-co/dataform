import { dataform } from "@dataform/protos";
import { DistStyleTypes, ignoredProps, SortStyleTypes, TableTypes } from "./table";

const SQL_DATA_WAREHOUSE_DIST_HASH_REGEXP = new RegExp("HASH\\s*\\(\\s*\\w*\\s*\\)\\s*");

export function relativePath(path: string, base: string) {
  if (base.length == 0) {
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

export function variableNameFriendly(value: string) {
  return value
    .replace("-", "")
    .replace("@", "")
    .replace("/", "");
}

export function matchPatterns(patterns: string[], values: string[]) {
  const regexps = patterns.map(
    pattern =>
      new RegExp(
        "^" +
          pattern
            .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
            .split("*")
            .join(".*") +
          "$"
      )
  );
  return values.filter(value => regexps.filter(regexp => regexp.test(value)).length > 0);
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

  // Check there aren't any duplicate names.
  const allActions = [].concat(
    compiledGraph.tables,
    compiledGraph.assertions,
    compiledGraph.operations
  );
  const allActionNames = allActions.map(action => action.name);

  // Check there are no duplicate action names.
  allActions.forEach(action => {
    if (allActions.filter(subAction => subAction.name == action.name).length > 1) {
      const actionName = action.name;
      const message = `Duplicate action name detected, names must be unique across tables, assertions, and operations: "${action.name}"`;
      validationErrors.push(dataform.ValidationError.create({ message, actionName }));
    }
  });

  const actionsByName: { [name: string]: dataform.IExecutionAction } = {};
  allActions.forEach(action => (actionsByName[action.name] = action));

  // Check all dependencies actually exist.
  allActions.forEach(action => {
    const actionName = action.name;
    (action.dependencies || []).forEach((dependency: string) => {
      if (allActionNames.indexOf(dependency) < 0) {
        const message = `Missing dependency detected: Node "${action.name}" depends on "${dependency}" which does not exist.`;
        validationErrors.push(dataform.ValidationError.create({ message, actionName }));
      }
    });
  });

  // Check for circular dependencies.
  const checkCircular = (
    action: dataform.IExecutionAction,
    dependents: dataform.IExecutionAction[]
  ): boolean => {
    if (dependents.indexOf(action) >= 0) {
      const actionName = action.name;
      const message = `Circular dependency detected in chain: [${dependents
        .map(d => d.name)
        .join(" > ")} > ${action.name}]`;
      validationErrors.push(dataform.ValidationError.create({ message, actionName }));
      return true;
    }
    return (action.dependencies || []).some(d => {
      return actionsByName[d] && checkCircular(actionsByName[d], dependents.concat([action]));
    });
  };

  for (const action of allActions) {
    if (checkCircular(action, [])) {
      break;
    }
  }

  // Table validation
  compiledGraph.tables.forEach(action => {
    const actionName = action.name;

    // type
    if (!!action.type && !Object.values(TableTypes).includes(action.type)) {
      const predefinedTypes = joinQuoted(Object.values(TableTypes));
      const message = `Wrong type of table detected. Should only use predefined types: ${predefinedTypes}`;
      validationErrors.push(dataform.ValidationError.create({ message, actionName }));
    }

    // "where" property
    if (action.type === TableTypes.INCREMENTAL && (!action.where || action.where.length === 0)) {
      const message = `"where" property is not defined. With the type “incremental” you must also specify the property “where”!`;
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
      if (
        Object.keys(action.redshift).length === 0 ||
        Object.values(action.redshift).every((value: string) => !value.length)
      ) {
        const message = `Missing properties in redshift config`;
        validationErrors.push(dataform.ValidationError.create({ message, actionName }));
      }

      const validatePropertyDefined = (
        opts: dataform.IRedshiftOptions,
        prop: keyof dataform.IRedshiftOptions
      ) => {
        if (!opts[prop] || !opts[prop].length) {
          const message = `Property "${prop}" is not defined`;
          validationErrors.push(dataform.ValidationError.create({ message, actionName }));
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
