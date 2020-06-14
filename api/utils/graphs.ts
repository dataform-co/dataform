import { JSONObjectStringifier, StringifiedMap } from "df/common/strings/stringifier";
import { dataform } from "df/protos/ts";

export const combineAllActions = (graph: dataform.ICompiledGraph) => {
  return ([] as Array<
    dataform.ITable | dataform.IOperation | dataform.IAssertion | dataform.IDeclaration
  >).concat(
    graph.tables || ([] as dataform.ITable[]),
    graph.operations || ([] as dataform.IOperation[]),
    graph.assertions || ([] as dataform.IAssertion[]),
    graph.declarations || ([] as dataform.IDeclaration[])
  );
};

export function actionsByTarget(compiledGraph: dataform.ICompiledGraph) {
  return new StringifiedMap(
    JSONObjectStringifier.create<dataform.ITarget>(),
    combineAllActions(compiledGraph)
      // Required for backwards compatibility with old versions of @dataform/core.
      .filter(action => !!action.target)
      .map(action => [action.target, action])
  );
}

export function actionsByCanonicalTarget(compiledGraph: dataform.ICompiledGraph) {
  return new StringifiedMap(
    JSONObjectStringifier.create<dataform.ITarget>(),
    combineAllActions(compiledGraph)
      // Required for backwards compatibility with old versions of @dataform/core.
      .filter(action => !!action.canonicalTarget)
      .map(action => [action.canonicalTarget, action])
  );
}
