import * as fs from "fs";
import * as path from "path";

export function getRealPath(targetPath: string): string {
  try {
    return fs.realpathSync(targetPath);
  } catch {
    return path.resolve(targetPath);
  }
}
