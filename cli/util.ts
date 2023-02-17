import * as fs from "fs";
import * as path from "path";

import * as core from "df/protos/core";
import * as execution from "df/protos/execution";
import untildify from "untildify";

export const actuallyResolve = (filePath: string) => path.resolve(untildify(filePath));

export function assertPathExists(checkPath: string) {
  if (!fs.existsSync(checkPath)) {
    throw new Error(`${checkPath} does not exist!`);
  }
}

export function compiledGraphHasErrors(graph: dataform.CompiledGraph) {
  return graph.graphErrors?.compilationErrors?.length > 0;
}
