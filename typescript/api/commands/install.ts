import * as path from "path";
import * as childProcess from "child_process";
import { promisify } from "util";

export function install(projectDir: string) {
  return promisify(childProcess.exec)("npm i", { cwd: path.resolve(projectDir) });
}
