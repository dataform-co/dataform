import * as path from "path";
import { promisify } from "util";

import * as childProcess from "child_process";

export async function install(projectDir: string, skipInstall?: boolean) {
  if (skipInstall) {
    return;
  }
  await promisify(childProcess.exec)("npm i --ignore-scripts", { cwd: path.resolve(projectDir) });
}
