import { IActionProto } from "df/core/session";
import { dataform } from "df/protos/ts";

export function actionsByStringifiedTarget(compiledGraph: dataform.ICompiledGraph) {
  return new Map<string, IActionProto>(
    ([] as IActionProto[])
      .concat(
        compiledGraph.tables,
        compiledGraph.operations,
        compiledGraph.assertions,
        compiledGraph.declarations
      )
      .map(action => [JSON.stringify(action.target), action])
  );
}
