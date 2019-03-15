import * as protos from "@dataform/protos";
import { TableTypes, DistStyleTypes, SortStyleTypes, ignoredProps } from "./table";

export function relativePath(path: string, base: string) {
  if (base.length == 0) {
    return path;
  }
  var stripped = path.substr(base.length);
  if (stripped.startsWith("/")) {
    return stripped.substr(1);
  } else {
    return stripped;
  }
}

export function baseFilename(path: string) {
  var pathSplits = path.split("/");
  return pathSplits[pathSplits.length - 1].split(".")[0];
}

export function variableNameFriendly(value: string) {
  return value
    .replace("-", "")
    .replace("@", "")
    .replace("/", "");
}

export function matchPatterns(patterns: string[], values: string[]) {
  var regexps = patterns.map(
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
      )
        break;
    }
  } catch (e) {}
  Error.prepareStackTrace = originalFunc;

  return relativePath(callerfile || lastfile, rootDir);
}

export function graphHasErrors(graph: protos.ICompiledGraph) {
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

function isPropertyExist(prop: any): boolean {
  if (!prop) {
    return false;
  }

  return (
    (Array.isArray(prop) && !!prop.length) ||
    (!Array.isArray(prop) && typeof prop === "object" && !!Object.keys(prop).length) ||
    typeof prop !== "object"
  );
}

export function validate(compiledGraph: protos.ICompiledGraph): protos.IGraphErrors {
  const validationErrors: protos.IValidationError[] = [];

  // Check there aren't any duplicate names.
  var allNodes = [].concat(compiledGraph.tables, compiledGraph.assertions, compiledGraph.operations);
  var allNodeNames = allNodes.map(node => node.name);

  // Check there are no duplicate node names.
  allNodes.forEach(node => {
    if (allNodes.filter(subNode => subNode.name == node.name).length > 1) {
      const nodeName = node.name;
      const message = `Duplicate node name detected, names must be unique across tables, assertions, and operations: "${
        node.name
      }"`;
      validationErrors.push(protos.ValidationError.create({ message, nodeName }));
    }
  });

  // Expand node dependency wilcards.
  allNodes.forEach(node => {
    const uniqueDeps: { [d: string]: boolean } = {};
    const deps = node.dependencies || [];
    // Add non-wildcard deps normally.
    deps.filter(d => !d.includes("*")).forEach(d => (uniqueDeps[d] = true));
    // Match wildcard deps against all node names.
    matchPatterns(deps.filter(d => d.includes("*")), allNodeNames).forEach(d => (uniqueDeps[d] = true));
    node.dependencies = Object.keys(uniqueDeps);
  });

  var nodesByName: { [name: string]: protos.IExecutionNode } = {};
  allNodes.forEach(node => (nodesByName[node.name] = node));

  // Check all dependencies actually exist.
  allNodes.forEach(node => {
    const nodeName = node.name;
    node.dependencies.forEach(dependency => {
      if (allNodeNames.indexOf(dependency) < 0) {
        const message = `Missing dependency detected: Node "${
          node.name
        }" depends on "${dependency}" which does not exist.`;
        validationErrors.push(protos.ValidationError.create({ message, nodeName }));
      }
    });
  });

  // Check for circular dependencies.
  const checkCircular = (node: protos.IExecutionNode, dependents: protos.IExecutionNode[]): boolean => {
    if (dependents.indexOf(node) >= 0) {
      const nodeName = node.name;
      const message = `Circular dependency detected in chain: [${dependents.map(d => d.name).join(" > ")} > ${
        node.name
      }]`;
      validationErrors.push(protos.ValidationError.create({ message, nodeName }));
      return true;
    }
    return node.dependencies.some(d => {
      return nodesByName[d] && checkCircular(nodesByName[d], dependents.concat([node]));
    });
  };

  for (let i = 0; i < allNodes.length; i++) {
    if (checkCircular(allNodes[i], [])) {
      break;
    }
  }

  // Table validation
  compiledGraph.tables.forEach(node => {
    const nodeName = node.name;

    // type
    if (
      !!node.type &&
      Object.keys(TableTypes)
        .map(key => TableTypes[key])
        .indexOf(node.type) === -1
    ) {
      const predefinedTypes = getPredefinedTypes(TableTypes);
      const message = `Wrong type of table detected. Should only use predefined types: ${predefinedTypes}`;
      validationErrors.push(protos.ValidationError.create({ message, nodeName }));
    }

    // "where" property
    if (node.type === TableTypes.INCREMENTAL && (!node.where || node.where.length === 0)) {
      const message = `"where" property is not defined. With the type “incremental” you must also specify the property “where”!`;
      validationErrors.push(protos.ValidationError.create({ message, nodeName }));
    }

    // redshift config
    if (!!node.redshift) {
      if (
        Object.keys(node.redshift).length === 0 ||
        Object.keys(node.redshift).every(key => !node.redshift[key] || !node.redshift[key].length)
      ) {
        const message = `Missing properties in redshift config`;
        validationErrors.push(protos.ValidationError.create({ message, nodeName }));
      }
      const redshiftConfig = [];

      if (node.redshift.distStyle || node.redshift.distKey) {
        const props = { distStyle: node.redshift.distStyle, distKey: node.redshift.distKey };
        const types = { distStyle: DistStyleTypes };
        redshiftConfig.push({ props, types });
      }
      if (node.redshift.sortStyle || (node.redshift.sortKeys && node.redshift.sortKeys.length)) {
        const props = { sortStyle: node.redshift.sortStyle, sortKeys: node.redshift.sortKeys };
        const types = { sortStyle: SortStyleTypes };
        redshiftConfig.push({ props, types });
      }

      redshiftConfig.forEach(item => {
        Object.keys(item.props).forEach(key => {
          if (!item.props[key] || !item.props[key].length) {
            const message = `Property "${key}" is not defined`;
            validationErrors.push(protos.ValidationError.create({ message, nodeName }));
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
            validationErrors.push(protos.ValidationError.create({ message, nodeName }));
          }
        });
      });
    }

    // ignored properties in tables
    if (!!ignoredProps[node.type]) {
      ignoredProps[node.type].forEach(ignoredProp => {
        if (isPropertyExist(node[ignoredProp])) {
          const message = `Unused property was detected: "${ignoredProp}". This property is not used for tables with type "${
            node.type
          }" and will be ignored.`;
          validationErrors.push(protos.ValidationError.create({ message, nodeName }));
        }
      });
    }
  });

  const compilationErrors =
    compiledGraph.graphErrors && compiledGraph.graphErrors.compilationErrors
      ? compiledGraph.graphErrors.compilationErrors
      : [];

  return protos.GraphErrors.create({ validationErrors, compilationErrors });
}
