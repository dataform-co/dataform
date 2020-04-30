import * as utils from "df/core/utils";
import { dataform } from "df/protos";

type CompileAction = dataform.ITable | dataform.IOperation | dataform.IAssertion;

export function prune(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig
): dataform.ICompiledGraph {
  const includedActionNames = computeIncludedActionNames(compiledGraph, runConfig);
  return {
    ...compiledGraph,
    tables: compiledGraph.tables
      .filter(action => includedActionNames.has(action.name))
      .map(action => cleanDependencies(action, includedActionNames)),
    assertions: compiledGraph.assertions
      .filter(action => includedActionNames.has(action.name))
      .map(action => cleanDependencies(action, includedActionNames)),
    operations: compiledGraph.operations
      .filter(action => includedActionNames.has(action.name))
      .map(action => cleanDependencies(action, includedActionNames))
  };
}
function cleanDependencies<T extends CompileAction>(
  action: T,
  includedActionNames: Set<string>
): T {
  // Remove any excluded dependencies.
  return {
    ...action,
    dependencies: (action.dependencies || []).filter(dep => includedActionNames.has(dep))
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

  const allActionNames = allActions.map(n => n.name);
  const allActionsByName: { [name: string]: CompileAction } = {};
  allActions.forEach(action => (allActionsByName[action.name] = action));

  const hasActionSelector = runConfig.actions && runConfig.actions.length > 0;
  const hasTagSelector = runConfig.tags && runConfig.tags.length > 0;

  // If no selectors, return all actions.
  if (!hasActionSelector && !hasTagSelector) {
    return new Set<string>(allActionNames);
  }

  const includedActionNames = new Set<string>();

  // Add all actions included by action filters.
  if (hasActionSelector) {
    utils
      .matchPatterns(runConfig.actions, allActionNames)
      .forEach(actionName => includedActionNames.add(actionName));
  }

  // Determine actions selected with --tag option and update applicable actions
  if (hasTagSelector) {
    allActions
      .filter(action => action.tags.some(tag => runConfig.tags.includes(tag)))
      .forEach(action => includedActionNames.add(action.name));
  }

  // Compute all transitive dependencies.
  if (runConfig.includeDependencies) {
    const queue = [...includedActionNames];
    while (queue.length > 0) {
      const actionName = queue.pop();
      const action = allActionsByName[actionName];
      const matchingDependencyNames =
        action.dependencies && action.dependencies.length > 0
          ? utils.matchPatterns(action.dependencies, allActionNames)
          : [];
      matchingDependencyNames.forEach(dependencyName => {
        if (!includedActionNames.has(dependencyName)) {
          queue.push(dependencyName);
          includedActionNames.add(dependencyName);
        }
      });
    }
  }

  return includedActionNames;
}
