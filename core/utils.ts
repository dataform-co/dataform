import { dataform } from "@dataform/protos";
import { Assertion } from "@dataform/core/assertion";
import { Operation } from "@dataform/core/operation";
import { Resolvable } from "@dataform/core/session";
import { Table } from "@dataform/core/table";

export const SQL_DATA_WAREHOUSE_DIST_HASH_REGEXP = new RegExp("HASH\\s*\\(\\s*\\w*\\s*\\)\\s*");

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
  const fQActs: string[] = [];
  patterns.forEach(pat => {
    if (pat.includes(".")) {
      if (values.includes(pat)) {
        fQActs.push(pat);
      }
    } else {
      const matchingActions = values.filter(value => pat === value.split(".").slice(-1)[0]);
      if (matchingActions.length === 0) {
        return;
      }
      if (matchingActions.length > 1) {
        throw new Error(ambiguousActionNameMsg(pat, matchingActions));
      }
      fQActs.push(matchingActions[0]);
    }
  });
  return fQActs;
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

  return graphErrors.compilationErrors && graphErrors.compilationErrors.length > 0;
}

export function joinQuoted(values: string[]) {
  return values.map((value: string) => `"${value}"`).join(" | ");
}

export function objectExistsOrIsNonEmpty(prop: any): boolean {
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
  // Check there aren't any duplicate names.
  const allActions = [].concat(
    compiledGraph.tables,
    compiledGraph.assertions,
    compiledGraph.operations
  );

  const actionsByName: { [name: string]: dataform.IExecutionAction } = {};
  allActions.forEach(action => (actionsByName[action.name] = action));

  const compilationErrors =
    compiledGraph.graphErrors && compiledGraph.graphErrors.compilationErrors
      ? compiledGraph.graphErrors.compilationErrors
      : [];

  return dataform.GraphErrors.create({ compilationErrors });
}

export function flatten<T>(nestedArray: T[][]) {
  return nestedArray.reduce((previousValue: T[], currentValue: T[]) => {
    return previousValue.concat(currentValue);
  }, []);
}

export function isResolvable(res: any) {
  return typeof res === "string" || (!!res.schema && !!res.name);
}

export function stringifyResolvable(res: Resolvable) {
  return typeof res === "string" ? res : `${res.schema}.${res.name}`;
}

export function targetAsResolvable(t: dataform.ITarget) {
  return { schema: t.schema, name: t.name };
}

export function ambiguousActionNameMsg(
  act: Resolvable,
  allActs: Array<Table | Operation | Assertion> | string[]
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
