import { JSONObjectStringifier, StringifiedMap } from "df/common/strings/stringifier";
import { IActionProto } from "df/core/session";
import { dataform } from "df/protos/ts";

export function actionsByTarget(compiledGraph: dataform.ICompiledGraph) {
  return new StringifiedMap(
    JSONObjectStringifier.create<dataform.ITarget>(),
    ([] as IActionProto[])
      .concat(
        compiledGraph.tables,
        compiledGraph.operations,
        compiledGraph.assertions,
        compiledGraph.declarations
      )
      // Required for backwards compatibility with old versions of @dataform/core.
      .filter(action => !!action.target)
      .map(action => [action.target, action])
  );
}
