import * as path from "path";
import * as childProcess from "child_process";
import { promisify } from "util";

export function install(projectDir: string, skipInstall?: boolean) {
  if (skipInstall) {
    return;
  }
  return promisify(childProcess.exec)("npm i", { cwd: path.resolve(projectDir) });
}
