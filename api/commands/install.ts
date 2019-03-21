import * as path from "path";
import * as childProcess from "child_process";
import { promisify } from "util";

export async function install(projectDir: string, skipInstall?: boolean) {
  if (skipInstall) {
    return;
  }
  await promisify(childProcess.exec)("npm i", { cwd: path.resolve(projectDir) });
}
