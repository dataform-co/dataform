import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

type CompileAction = dataform.ITable | dataform.IOperation | dataform.IAssertion;

export function prune(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig
): dataform.ICompiledGraph {
  const includedActionNames = computeIncludedActionTargets(compiledGraph, runConfig);
  return {
    ...compiledGraph,
    tables: compiledGraph.tables.filter(action => includedActionNames.has(action.target)),
    assertions: compiledGraph.assertions.filter(action => includedActionNames.has(action.target)),
    operations: compiledGraph.operations.filter(action => includedActionNames.has(action.target))
  };
}

function computeIncludedActionTargets(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig
): Set<dataform.ITarget> {
  // Remove inline tables.
  const filteredTables = compiledGraph.tables.filter(t => t.type !== "inline");

  // Union all tables, operations, assertions.
  const allActions: CompileAction[] = [].concat(
    filteredTables,
    compiledGraph.operations,
    compiledGraph.assertions
  );

  const allActionTargets = allActions.map(n => n.target);
  const allActionsByName: { [name: string]: CompileAction } = {};
  allActions.forEach(action => (allActionsByName[utils.targetToName(action.target)] = action));

  const hasActionSelector = runConfig.actions && runConfig.actions.length > 0;
  const hasTagSelector = runConfig.tags && runConfig.tags.length > 0;

  // If no selectors, return all actions.
  if (!hasActionSelector && !hasTagSelector) {
    return new Set<dataform.ITarget>(allActionTargets);
  }

  const includedActionNames = new Set<dataform.ITarget>();

  // Add all actions included by action filters.
  if (hasActionSelector) {
    utils
      .matchPatterns(runConfig.actions, allActionTargets)
      .forEach(actionTarget => includedActionNames.add(actionTarget));
  }

  // Determine actions selected with --tag option and update applicable actions
  if (hasTagSelector) {
    allActions
      .filter(action => action.tags.some(tag => runConfig.tags.includes(tag)))
      .forEach(action => includedActionNames.add(action.target));
  }

  // Compute all transitive dependencies.
  if (runConfig.includeDependencies) {
    const queue = [...includedActionNames];
    while (queue.length > 0) {
      const actionName = utils.targetToName(queue.pop());
      const action = allActionsByName[actionName];
      const matchingDependencyNames =
        action.dependencies && action.dependencies.length > 0
          ? utils.matchPatterns(action.dependencies, allActionTargets)
          : [];
      matchingDependencyNames.forEach(dependencyName => {
        if (!includedActionNames.has(dependencyName)) {
          queue.push(dependencyName);
          includedActionNames.add(dependencyName);
        }
      });
    }
  }

  // Add auto assertions
  [...compiledGraph.assertions].forEach(assertion => {
    if (!!assertion.parentAction) {
      if (includedActionNames.has(assertion.parentAction)) {
        includedActionNames.add(assertion.target);
      }
    }
  });

  return includedActionNames;
}
