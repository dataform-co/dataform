import { dataform } from "@dataform/protos";
import * as fs from "fs";
import * as path from "path";
import * as untildify from "untildify";

export const actuallyResolve = filePath => path.resolve(untildify(filePath));

export function assertPathExists(checkPath: string) {
  if (!fs.existsSync(checkPath)) {
    throw new Error(`${checkPath} does not exist!`);
  }
}

export function compiledGraphHasErrors(graph: dataform.ICompiledGraph) {
  if (!graph.graphErrors) {
    return false;
  }
  return (
    (graph.graphErrors.compilationErrors && graph.graphErrors.compilationErrors.length > 0) ||
    (graph.graphErrors.validationErrors && graph.graphErrors.validationErrors.length > 0)
  );
}
