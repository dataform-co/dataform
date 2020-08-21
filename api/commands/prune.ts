import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

type CompileAction = dataform.ITable | dataform.IOperation | dataform.IAssertion;

export function prune(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig
): dataform.ICompiledGraph {
  const includedActionNames = computeIncludedActionNames(compiledGraph, runConfig);
  return {
    ...compiledGraph,
    tables: compiledGraph.tables.filter(action => includedActionNames.has(action.name)),
    assertions: compiledGraph.assertions.filter(action => includedActionNames.has(action.name)),
    operations: compiledGraph.operations.filter(action => includedActionNames.has(action.name))
  };
}

function computeIncludedActionNames(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig
): Set<string> {
  // Remove inline tables.
  const filteredTables = compiledGraph.tables.filter(t => t.type !== "inline");

  // Union all tables, operations, assertions.
  const allActions: CompileAction[] = [].concat(
    filteredTables,
    compiledGraph.operations,
    compiledGraph.assertions
  );

  const allActionTargets = allActions.map(n => n.target || utils.nameToTarget(n.name));
  const allActionsByName: { [name: string]: CompileAction } = {};
  allActions.forEach(
    action =>
      (allActionsByName[!!action.target ? utils.targetToName(action.target) : action.name] = action)
  );

  const hasActionSelector = runConfig.actions && runConfig.actions.length > 0;
  const hasTagSelector = runConfig.tags && runConfig.tags.length > 0;

  // If no selectors, return all actions.
  if (!hasActionSelector && !hasTagSelector) {
    return new Set<string>(allActionTargets.map(target => utils.targetToName(target)));
  }

  const includedActionTargets = new Set<dataform.ITarget>();

  // Add all actions included by action filters.
  if (hasActionSelector) {
    utils
      .matchPatterns(runConfig.actions, allActionTargets)
      .forEach(actionTarget => includedActionTargets.add(actionTarget));
  }

  // Determine actions selected with --tag option and update applicable actions
  if (hasTagSelector) {
    allActions
      .filter(action => action.tags.some(tag => runConfig.tags.includes(tag)))
      .forEach(action => includedActionTargets.add(action.target));
  }

  // Compute all transitive dependencies.
  if (runConfig.includeDependencies) {
    const queue = [...includedActionTargets];
    while (queue.length > 0) {
      const actionName = utils.targetToName(queue.pop());
      const action = allActionsByName[actionName];
      const matchingDependencyNames =
        action.dependencies && action.dependencies.length > 0
          ? utils.matchPatterns(action.dependencies, allActionTargets)
          : [];
      matchingDependencyNames.forEach(dependencyName => {
        if (!includedActionTargets.has(dependencyName)) {
          queue.push(dependencyName);
          includedActionTargets.add(dependencyName);
        }
      });
    }
  }

  // Add auto assertions
  [...compiledGraph.assertions].forEach(assertion => {
    if (!!assertion.parentAction) {
      if (includedActionTargets.has(assertion.parentAction)) {
        includedActionTargets.add(assertion.target);
      }
    }
  });

  const includedActionNames = new Set<string>();
  includedActionTargets.forEach(target => includedActionNames.add(utils.targetToName(target)));

  return includedActionNames;
}
