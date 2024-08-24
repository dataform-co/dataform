import * as fs from "fs";
import * as path from "path";

import { dataform } from "#df/protos/ts";
import untildify from "untildify";

export function actuallyResolve(...filePaths: string[]) {
  return path.resolve(...filePaths.map(filePath => untildify(filePath)));
}

export function assertPathExists(checkPath: string) {
  if (!fs.existsSync(checkPath)) {
    throw new Error(`${checkPath} does not exist!`);
  }
}

export function compiledGraphHasErrors(graph: dataform.ICompiledGraph) {
  return graph.graphErrors?.compilationErrors?.length > 0;
}
