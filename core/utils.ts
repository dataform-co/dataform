import { dataform } from "@dataform/protos";
import { DistStyleTypes, ignoredProps, SortStyleTypes, TableTypes } from "./table";

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
  const originalFunc = Error.prepareStackTrace;
  let callerfile;
  let lastfile;
  try {
    const err = new Error();
    let currentfile;
    Error.prepareStackTrace = function(err, stack) {
      return stack;
    };

    currentfile = (err.stack as any).shift().getFileName();
    while (err.stack.length) {
      callerfile = (err.stack as any).shift().getFileName();
      if (callerfile) {
        lastfile = callerfile;
      }
      if (
        currentfile !== callerfile &&
        callerfile.includes(rootDir) &&
        !callerfile.includes("node_modules") &&
        // We don't want to attribute files in includes/ to the caller files.
        (callerfile.includes("definitions/") || callerfile.includes("models/"))
      ) {
        break;
      }
    }
  } catch (e) { }
  Error.prepareStackTrace = originalFunc;

  return relativePath(callerfile || lastfile, rootDir);
}

export function graphHasErrors(graph: dataform.ICompiledGraph) {
  const graphErrors = validate(graph);

  return (
    (graphErrors.compilationErrors && graphErrors.compilationErrors.length > 0) ||
    (graphErrors.validationErrors && graphErrors.validationErrors.length > 0)
  );
}

function getPredefinedTypes(types): string {
  return Object.keys(types)
    .map(key => `"${types[key]}"`)
    .join(" | ");
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
      const message = `Duplicate action name detected, names must be unique across tables, assertions, and operations: "${
        action.name
      }"`;
      validationErrors.push(dataform.ValidationError.create({ message, actionName }));
    }
  });

  const actionsByName: { [name: string]: dataform.IExecutionAction } = {};
  allActions.forEach(action => (actionsByName[action.name] = action));

  // Check all dependencies actually exist and are not ambiguous.
  allActions.forEach(action => {
    const actionName = action.name;
    (action.dependencies || []).forEach(dependency => {
      const [matchedDep, err] = matchFQName(dependency, allActionNames);
      if (!!err && err.includes("Ambiguous")) {
        const message = `Ambiguous dependency detected: ` + err;
        validationErrors.push(dataform.ValidationError.create({ message, actionName }));
      } else if (allActionNames.indexOf(matchedDep) < 0) {
        const message = `Missing dependency detected: Node "${
          action.name
        }" depends on "${dependency}" which does not exist.`;
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
    const depsFQ = [];
    (action.dependencies || []).forEach(d => {
      const [cleanDep, err] = matchFQName(d, allActionNames);
      if (!err) {
        depsFQ.push(cleanDep);
      }
    });
    return (depsFQ || []).some(d => {
      return actionsByName[d] && checkCircular(actionsByName[d], dependents.concat([action]));
    });
  };

  for (let i = 0; i < allActions.length; i++) {
    if (checkCircular(allActions[i], [])) {
      break;
    }
  }

  // Table validation
  compiledGraph.tables.forEach(action => {
    const actionName = action.name;

    // type
    if (
      !!action.type &&
      Object.keys(TableTypes)
        .map(key => TableTypes[key])
        .indexOf(action.type) === -1
    ) {
      const predefinedTypes = getPredefinedTypes(TableTypes);
      const message = `Wrong type of table detected. Should only use predefined types: ${predefinedTypes}`;
      validationErrors.push(dataform.ValidationError.create({ message, actionName }));
    }

    // "where" property
    if (action.type === TableTypes.INCREMENTAL && (!action.where || action.where.length === 0)) {
      const message = `"where" property is not defined. With the type “incremental” you must also specify the property “where”!`;
      validationErrors.push(dataform.ValidationError.create({ message, actionName }));
    }

    // redshift config
    if (!!action.redshift) {
      if (
        Object.keys(action.redshift).length === 0 ||
        Object.keys(action.redshift).every(
          key => !action.redshift[key] || !action.redshift[key].length
        )
      ) {
        const message = `Missing properties in redshift config`;
        validationErrors.push(dataform.ValidationError.create({ message, actionName }));
      }
      const redshiftConfig = [];

      if (action.redshift.distStyle || action.redshift.distKey) {
        const props = { distStyle: action.redshift.distStyle, distKey: action.redshift.distKey };
        const types = { distStyle: DistStyleTypes };
        redshiftConfig.push({ props, types });
      }
      if (
        action.redshift.sortStyle ||
        (action.redshift.sortKeys && action.redshift.sortKeys.length)
      ) {
        const props = { sortStyle: action.redshift.sortStyle, sortKeys: action.redshift.sortKeys };
        const types = { sortStyle: SortStyleTypes };
        redshiftConfig.push({ props, types });
      }

      redshiftConfig.forEach(item => {
        Object.keys(item.props).forEach(key => {
          if (!item.props[key] || !item.props[key].length) {
            const message = `Property "${key}" is not defined`;
            validationErrors.push(dataform.ValidationError.create({ message, actionName }));
          }
        });

        Object.keys(item.types).forEach(type => {
          const currentEnum = item.types[type];
          if (
            !!item.props[type] &&
            Object.keys(currentEnum)
              .map(key => currentEnum[key])
              .indexOf(item.props[type]) === -1
          ) {
            const predefinedValues = getPredefinedTypes(currentEnum);
            const message = `Wrong value of "${type}" property. Should only use predefined values: ${predefinedValues}`;
            validationErrors.push(dataform.ValidationError.create({ message, actionName }));
          }
        });
      });
    }

    // ignored properties in tables
    if (!!ignoredProps[action.type]) {
      ignoredProps[action.type].forEach(ignoredProp => {
        if (objectExistsOrIsNonEmpty(action[ignoredProp])) {
          const message = `Unused property was detected: "${ignoredProp}". This property is not used for tables with type "${
            action.type
          }" and will be ignored.`;
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

export function matchFQName(act: string, allActFQNames: any[]): [string, string] {
  switch (act.split(".").length) {
    case 2: {
      if (allActFQNames.includes(act)) {
        return [act, null]; // Fully Qualified name match. Return as it is.
      } else {
        return [null, "Action name: " + act + " could not be found."];
      }
    }
    case 1: {
      const allActShortNamesMap = allActFQNames.map(actFQ => [
        actFQ,
        actFQ.includes(".") ? actFQ.split(".")[1] : actFQ
      ]);
      const matches = [];
      allActShortNamesMap
        .filter(actShort => actShort[1] === act)
        .forEach(actShort => matches.push(actShort[0]));
      if (matches.length === 0) {
        return [null, "Action name: " + act + " could not be found."]; // No matches.
      } else if (matches.length === 1) {
        return [matches[0], null]; // There was exactly one match to the short name. Return the full name.
      } else if (matches.length > 1) {
        return [
          null,
          "Ambiguous Action name: " + act + ". Did you mean one of: [" + matches.join(", ") + "]."
        ];
      }
    }
    default: {
      return [null, "Action name: " + act + " is invalid."];
    }
  }
}
