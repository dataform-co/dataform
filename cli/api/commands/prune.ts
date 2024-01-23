import { targetAsReadableString } from "df/core/targets";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

type CompileAction = dataform.ITable | dataform.IOperation | dataform.IAssertion;

export function prune(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig
): dataform.ICompiledGraph {
  compiledGraph.tables.forEach(utils.setOrValidateTableEnumType);
  const includedActionNames = computeIncludedActionNames(compiledGraph, runConfig);
  return {
    ...compiledGraph,
    tables: compiledGraph.tables.filter(action =>
      includedActionNames.has(targetAsReadableString(action.target))
    ),
    assertions: compiledGraph.assertions.filter(action =>
      includedActionNames.has(targetAsReadableString(action.target))
    ),
    operations: compiledGraph.operations.filter(action =>
      includedActionNames.has(targetAsReadableString(action.target))
    )
  };
}

function computeIncludedActionNames(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig
): Set<string> {
  // Union all tables, operations, assertions.
  const allActions: CompileAction[] = [].concat(
    compiledGraph.tables,
    compiledGraph.operations,
    compiledGraph.assertions
  );

  const allActionNames = new Set<string>(
    allActions.map(action => targetAsReadableString(action.target))
  );
  const allActionsByName = new Map<string, CompileAction>(
    allActions.map(action => [targetAsReadableString(action.target), action])
  );

  const hasActionSelector = runConfig.actions?.length > 0;
  const hasTagSelector = runConfig.tags?.length > 0;

  // If no selectors, return all actions.
  if (!hasActionSelector && !hasTagSelector) {
    return allActionNames;
  }

  const includedActionNames = new Set<string>();

  // Add all actions included by action filters.
  if (hasActionSelector) {
    utils
      .matchPatterns(runConfig.actions, [...allActionNames])
      .forEach(actionName => includedActionNames.add(actionName));
  }

  // Determine actions selected with --tag option and update applicable actions
  if (hasTagSelector) {
    allActions
      .filter(action => action.tags.some(tag => runConfig.tags.includes(tag)))
      .forEach(action => includedActionNames.add(targetAsReadableString(action.target)));
  }

  // Compute all transitive dependencies.
  if (runConfig.includeDependencies) {
    const queue = [...includedActionNames];
    while (queue.length > 0) {
      const actionName = queue.pop();
      const action = allActionsByName.get(actionName);
      const matchingDependencyNames =
        action.dependencyTargets?.length > 0
          ? utils.matchPatterns(
              action.dependencyTargets.map(dependency => targetAsReadableString(dependency)),
              [...allActionNames]
            )
          : [];
      matchingDependencyNames.forEach(dependencyName => {
        if (!includedActionNames.has(dependencyName)) {
          queue.push(dependencyName);
          includedActionNames.add(dependencyName);
        }
      });
    }
  }

  // Compute all transitive dependents.
  if (runConfig.includeDependents) {
    const queue = [...includedActionNames];
    while (queue.length > 0) {
      const actionName = queue.pop();
      const matchingDependentNames = allActions
        .filter(
          compileAction =>
            utils.matchPatterns(
              [actionName],
              compileAction.dependencyTargets?.map(dependency =>
                targetAsReadableString(dependency)
              ) || []
            ).length >= 1
        )
        .map(compileAction => targetAsReadableString(compileAction.target));
      matchingDependentNames.forEach(dependentName => {
        if (!includedActionNames.has(dependentName)) {
          queue.push(dependentName);
          includedActionNames.add(dependentName);
        }
      });
    }
  }

  return includedActionNames;
}
