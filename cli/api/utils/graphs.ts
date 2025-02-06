import { targetStringifier } from "df/core/targets";
import { dataform } from "df/protos/ts";

type CoreProtoActionTypes =
  | dataform.ITable
  | dataform.IOperation
  | dataform.IAssertion
  | dataform.IDeclaration
  | dataform.IDataPreparation;

function combineAllActions(graph: dataform.ICompiledGraph) {
  return ([] as Array<CoreProtoActionTypes>).concat(
    graph.tables || ([] as dataform.ITable[]),
    graph.operations || ([] as dataform.IOperation[]),
    graph.assertions || ([] as dataform.IAssertion[]),
    graph.declarations || ([] as dataform.IDeclaration[]),
    graph.dataPreparations || ([] as dataform.IDataPreparation[])
  );
}

export function actionsByTarget(compiledGraph: dataform.ICompiledGraph) {
  const actionsMap = new Map<string, CoreProtoActionTypes>();
  combineAllActions(compiledGraph)
    // Required for backwards compatibility with old versions of @dataform/core.
    .filter(action => !!action.target)
    .forEach(action => {
      actionsMap.set(targetStringifier.stringify(action.target), action);
    });
}

export function actionsByCanonicalTarget(compiledGraph: dataform.ICompiledGraph) {
  const actionsMap = new Map<string, CoreProtoActionTypes>();
  combineAllActions(compiledGraph)
    // Required for backwards compatibility with old versions of @dataform/core.
    .filter(action => !!action.canonicalTarget)
    .forEach(action => {
      actionsMap.set(targetStringifier.stringify(action.canonicalTarget), action);
    });
}
