import { JSONObjectStringifier, StringifiedMap } from "df/common/strings/stringifier";
import { IActionProto } from "df/core/session";
import { dataform } from "df/protos/ts";

const types = ["dataset", "assertion", "operation", "declaration"] as const;
export type Type = typeof types[number];

export interface IAction {
  name: string;
  type: Type;
  fileName: string;
  dependencies?: string[];
  target?: dataform.ITarget;
  canonicalTarget?: dataform.ITarget;
  actionDescriptor?: dataform.IActionDescriptor;
  tags?: string[];
  query?: string;
  queries?: string[];
}

export const convertGraphToActions = (graph: dataform.ICompiledGraph) => {
  return [].concat(
    (graph.tables || []).map(t => ({ ...t, type: "dataset" })),
    (graph.operations || []).map(o => ({ ...o, type: "operation" })),
    (graph.assertions || []).map(a => ({ ...a, type: "assertion" })),
    (graph.declarations || []).map(d => ({ ...d, type: "declaration" }))
  ) as IAction[];
};

export function actionsByTarget(compiledGraph: dataform.ICompiledGraph) {
  return new StringifiedMap(
    JSONObjectStringifier.create<dataform.ITarget>(),
    ([] as IActionProto[])
      .concat(
        compiledGraph.tables || [],
        compiledGraph.operations || [],
        compiledGraph.assertions || [],
        compiledGraph.declarations || []
      )
      // Required for backwards compatibility with old versions of @dataform/core.
      .filter(action => !!action.target)
      .map(action => [action.target, action])
  );
}

export function actionsByCanonicalTarget(compiledGraph: dataform.ICompiledGraph) {
  return new StringifiedMap(
    JSONObjectStringifier.create<dataform.ITarget>(),
    convertGraphToActions(compiledGraph)
      .filter(action => !!action.canonicalTarget)
      .map(action => [action.canonicalTarget, action])
  );
}
