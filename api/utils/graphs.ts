import { StringifiedMap } from "df/common/strings/stringifier";
import { targetStringifier } from "df/core/targets";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export const combineAllActions = (graph: core.CompiledGraph) => {
  return ([] as Array<core.Target | core.Operation | core.Assertion | core.Declaration>).concat(
    graph.tables || ([] as core.Target[]),
    graph.operations || ([] as core.Operation[]),
    graph.assertions || ([] as core.Assertion[]),
    graph.declarations || ([] as core.Declaration[])
  );
};

export function actionsByTarget(compiledGraph: core.CompiledGraph) {
  return new StringifiedMap(
    targetStringifier,
    combineAllActions(compiledGraph)
      // Required for backwards compatibility with old versions of @dataform/core.
      .filter(action => !!action.target)
      .map(action => [action.target, action])
  );
}

export function actionsByCanonicalTarget(compiledGraph: core.CompiledGraph) {
  return new StringifiedMap(
    targetStringifier,
    combineAllActions(compiledGraph)
      // Required for backwards compatibility with old versions of @dataform/core.
      .filter(action => !!action.canonicalTarget)
      .map(action => [action.canonicalTarget, action])
  );
}
