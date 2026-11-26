import { targetAsReadableString, targetStringifier } from "df/core/targets";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

type CompileAction =
  dataform.ITable | dataform.IOperation | dataform.IAssertion | dataform.IPropertyGraph;

function targetKey(target?: dataform.ITarget | null): string | undefined {
  return target ? targetStringifier.stringify(target) : undefined;
}

export function prune(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
): dataform.ICompiledGraph {
  (compiledGraph.tables || []).forEach(utils.setOrValidateTableEnumType);
  const includedActionKeys = computeIncludedActionKeys(compiledGraph, runConfig);
  const isIncluded = (target?: dataform.ITarget | null) => {
    const key = targetKey(target);
    return key !== undefined && includedActionKeys.has(key);
  };
  return {
    ...compiledGraph,
    tables: (compiledGraph.tables || []).filter((action) => isIncluded(action.target)),
    assertions: (compiledGraph.assertions || []).filter((action) => isIncluded(action.target)),
    operations: (compiledGraph.operations || []).filter((action) => isIncluded(action.target)),
    propertyGraphs: compiledGraph.propertyGraphs?.filter((action) => isIncluded(action.target)),
    // `targets` is declarative output only (nothing downstream reads it), but
    // `compile` prints it, so it has to agree with the filtered action lists.
    targets: compiledGraph.targets?.filter((target) => isIncluded(target)),
  };
}

function matchActionSelectors(patterns: string[], allActions: CompileAction[]): string[] {
  const matchedActionKeys: string[] = [];
  patterns.forEach((pattern) => {
    if (pattern.includes("*")) {
      // Wildcard selector: patterns containing "." match the full readable target string;
      // otherwise they match the unqualified action name, with no ambiguity error.
      const regExp = utils.globToRegExp(pattern);
      const matchFullTarget = pattern.includes(".");
      allActions.forEach((action) => {
        if (!action.target) {
          return;
        }
        const candidate = matchFullTarget
          ? targetAsReadableString(action.target)
          : action.target.name;
        if (candidate !== undefined && regExp.test(candidate)) {
          matchedActionKeys.push(targetStringifier.stringify(action.target));
        }
      });
    } else if (pattern.includes(".")) {
      allActions.forEach((action) => {
        if (action.target && targetAsReadableString(action.target) === pattern) {
          matchedActionKeys.push(targetStringifier.stringify(action.target));
        }
      });
    } else {
      const matchingActions = allActions.filter((action) => action.target?.name === pattern);
      if (matchingActions.length === 0) {
        return;
      }
      if (matchingActions.length > 1) {
        throw new Error(
          utils.ambiguousActionNameMsg(
            pattern,
            matchingActions.map((action) => targetAsReadableString(action.target)),
          ),
        );
      }
      matchedActionKeys.push(targetStringifier.stringify(matchingActions[0].target));
    }
  });
  return matchedActionKeys;
}

function computeIncludedActionKeys(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
): Set<string> {
  // Union all tables, operations, assertions, property graphs.
  const allActions: CompileAction[] = [].concat(
    compiledGraph.tables || [],
    compiledGraph.operations || [],
    compiledGraph.assertions || [],
    compiledGraph.propertyGraphs || [],
  );

  const allActionKeys = new Set<string>();
  const allActionsByKey = new Map<string, CompileAction>();
  allActions.forEach((action) => {
    const key = targetKey(action?.target);
    if (key !== undefined) {
      allActionKeys.add(key);
      allActionsByKey.set(key, action);
    }
  });

  const hasActionSelector = runConfig.actions?.length > 0;
  const hasTagSelector = runConfig.tags?.length > 0;

  // If no selectors, return all actions.
  if (!hasActionSelector && !hasTagSelector) {
    return allActionKeys;
  }

  const includedActionKeys = new Set<string>();

  // Add all actions included by action filters.
  if (hasActionSelector) {
    matchActionSelectors(runConfig.actions, allActions).forEach((actionKey) =>
      includedActionKeys.add(actionKey),
    );
  }

  // Determine actions selected with --tag option and update applicable actions
  if (hasTagSelector) {
    allActions
      .filter((action) => action.tags?.some((tag) => runConfig.tags.includes(tag)))
      .forEach((action) => {
        const key = targetKey(action.target);
        if (key !== undefined) {
          includedActionKeys.add(key);
        }
      });
  }

  // Compute all transitive dependencies.
  if (runConfig.includeDependencies) {
    const queue = [...includedActionKeys];
    while (queue.length > 0) {
      const actionKey = queue.pop();
      const action = allActionsByKey.get(actionKey);
      (action?.dependencyTargets || []).forEach((dependency) => {
        const dependencyKey = targetKey(dependency);
        if (
          dependencyKey !== undefined &&
          allActionKeys.has(dependencyKey) &&
          !includedActionKeys.has(dependencyKey)
        ) {
          queue.push(dependencyKey);
          includedActionKeys.add(dependencyKey);
        }
      });
    }
  }

  // Compute all transitive dependents.
  if (runConfig.includeDependents) {
    const queue = [...includedActionKeys];
    while (queue.length > 0) {
      const actionKey = queue.pop();
      allActions.forEach((compileAction) => {
        const dependsOnAction = compileAction.dependencyTargets?.some(
          (dependency) => targetKey(dependency) === actionKey,
        );
        if (dependsOnAction) {
          const dependentKey = targetKey(compileAction.target);
          if (dependentKey !== undefined && !includedActionKeys.has(dependentKey)) {
            queue.push(dependentKey);
            includedActionKeys.add(dependentKey);
          }
        }
      });
    }
  }

  return includedActionKeys;
}
