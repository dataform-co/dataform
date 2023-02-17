import { StringifiedMap } from "df/common/strings/stringifier";
import { targetStringifier } from "df/core/targets";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export const combineAllActions = (graph: dataform.CompiledGraph) => {
  return ([] as Array<core.Target | core.Operation | core.Assertion | dataform.Declaration>).concat(
    graph.tables || ([] as core.Target[]),
    graph.operations || ([] as core.Operation[]),
    graph.assertions || ([] as core.Assertion[]),
    graph.declarations || ([] as dataform.Declaration[])
  );
};

export function actionsByTarget(compiledGraph: dataform.CompiledGraph) {
  return new StringifiedMap(
    targetStringifier,
    combineAllActions(compiledGraph)
      // Required for backwards compatibility with old versions of @dataform/core.
      .filter(action => !!action.target)
      .map(action => [action.target, action])
  );
}

export function actionsByCanonicalTarget(compiledGraph: dataform.CompiledGraph) {
  return new StringifiedMap(
    targetStringifier,
    combineAllActions(compiledGraph)
      // Required for backwards compatibility with old versions of @dataform/core.
      .filter(action => !!action.canonicalTarget)
      .map(action => [action.canonicalTarget, action])
  );
}
