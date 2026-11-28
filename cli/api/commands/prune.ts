import { targetAsReadableString, targetStringifier } from "df/core/targets";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

type CompileAction =
  dataform.ITable | dataform.IOperation | dataform.IAssertion | dataform.IPropertyGraph;

function targetKey(target?: dataform.ITarget | null): string | undefined {
  return target ? targetStringifier.stringify(target) : undefined;
}

function formatTargetParts(target: dataform.ITarget): string {
  const parts = [
    target.database ? `database: "${target.database}"` : undefined,
    target.schema ? `schema: "${target.schema}"` : undefined,
    target.name ? `name: "${target.name}"` : undefined,
  ].filter(Boolean);
  return `{ ${parts.join(", ")} }`;
}

export function prune(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
): dataform.ICompiledGraph {
  (compiledGraph.tables || []).forEach(utils.setOrValidateTableEnumType);
  const { includedActionKeys, keyByAction } = computeIncludedActionKeys(compiledGraph, runConfig);
  const isActionIncluded = (action: CompileAction) => {
    const key = keyByAction.get(action);
    return key !== undefined && includedActionKeys.has(key);
  };
  const isTargetIncluded = (target?: dataform.ITarget | null) => {
    const key = targetKey(target);
    return key !== undefined && includedActionKeys.has(key);
  };
  return {
    ...compiledGraph,
    tables: (compiledGraph.tables || []).filter(isActionIncluded),
    assertions: (compiledGraph.assertions || []).filter(isActionIncluded),
    operations: (compiledGraph.operations || []).filter(isActionIncluded),
    propertyGraphs: compiledGraph.propertyGraphs?.filter(isActionIncluded),
    // `targets` is declarative output only (nothing downstream reads it), but
    // `compile` prints it, so it has to agree with the filtered action lists.
    targets: compiledGraph.targets?.filter(isTargetIncluded),
  };
}

function computeIncludedActionKeys(
  compiledGraph: dataform.ICompiledGraph,
  runConfig: dataform.IRunConfig,
): { includedActionKeys: Set<string>; keyByAction: Map<CompileAction, string> } {
  // Union all tables, operations, assertions, property graphs.
  const allActions: CompileAction[] = [].concat(
    compiledGraph.tables || [],
    compiledGraph.operations || [],
    compiledGraph.assertions || [],
    compiledGraph.propertyGraphs || [],
  );

  const allActionKeys = new Set<string>();
  const keyByAction = new Map<CompileAction, string>();
  const keyedActions: Array<{ action: CompileAction; key: string }> = [];
  allActions.forEach((action) => {
    const key = targetKey(action?.target);
    if (key !== undefined) {
      allActionKeys.add(key);
      keyByAction.set(action, key);
      keyedActions.push({ action, key });
    }
  });

  const hasActionSelector = runConfig.actions?.length > 0;
  const hasTagSelector = runConfig.tags?.length > 0;

  // If no selectors, return all actions.
  if (!hasActionSelector && !hasTagSelector) {
    return { includedActionKeys: allActionKeys, keyByAction };
  }

  const includedActionKeys = new Set<string>();

  // Add all actions included by action filters.
  if (hasActionSelector) {
    const candidates: utils.IMatchCandidate[] = keyedActions.map(({ action, key }) => {
      const fullName = targetAsReadableString(action.target);
      return {
        fullName,
        shortName: action.target.name,
        key,
        disambiguatedName: `${fullName} (${formatTargetParts(action.target)})`,
      };
    });
    utils
      .matchCandidatePatterns(runConfig.actions, candidates)
      .forEach((actionKey) => includedActionKeys.add(actionKey));
  }

  // Determine actions selected with --tag option and update applicable actions
  if (hasTagSelector) {
    keyedActions.forEach(({ action, key }) => {
      if (
        runConfig.tagsIntersection
          ? runConfig.tags.every((tag) => action.tags?.includes(tag))
          : action.tags?.some((tag) => runConfig.tags.includes(tag))
      ) {
        includedActionKeys.add(key);
      }
    });
  }

  const dependencyKeysByActionKey = new Map<string, string[]>();
  const dependentKeysByActionKey = new Map<string, string[]>();
  if (runConfig.includeDependencies || runConfig.includeDependents) {
    keyedActions.forEach(({ action, key }) => {
      const dependencyKeys: string[] = [];
      (action.dependencyTargets || []).forEach((dependency) => {
        const dependencyKey = targetKey(dependency);
        if (dependencyKey !== undefined && allActionKeys.has(dependencyKey)) {
          dependencyKeys.push(dependencyKey);
          let dependents = dependentKeysByActionKey.get(dependencyKey);
          if (!dependents) {
            dependents = [];
            dependentKeysByActionKey.set(dependencyKey, dependents);
          }
          dependents.push(key);
        }
      });
      dependencyKeysByActionKey.set(key, dependencyKeys);
    });
  }

  // Compute all transitive dependencies.
  if (runConfig.includeDependencies) {
    const queue = [...includedActionKeys];
    while (queue.length > 0) {
      const actionKey = queue.pop();
      (dependencyKeysByActionKey.get(actionKey) || []).forEach((dependencyKey) => {
        if (!includedActionKeys.has(dependencyKey)) {
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
      (dependentKeysByActionKey.get(actionKey) || []).forEach((dependentKey) => {
        if (!includedActionKeys.has(dependentKey)) {
          queue.push(dependentKey);
          includedActionKeys.add(dependentKey);
        }
      });
    }
  }

  return { includedActionKeys, keyByAction };
}
