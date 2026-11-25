import * as fs from "fs";
import * as path from "path";

/**
 * Resolves symbolic links in `targetPath`. A path that does not exist (a dangling link included)
 * is returned resolved but with its links unfollowed, so callers that must not accept such a path
 * should call `fs.realpathSync` themselves.
 */
export function getRealPath(targetPath: string): string {
  try {
    return fs.realpathSync(targetPath);
  } catch {
    return path.resolve(targetPath);
  }
}

/**
 * Whether `resolvedTargetPath` is equal to or nested inside `resolvedParentDir` (assuming both
 * paths have already been resolved to canonical real paths).
 */
export function isRealPathContained(
  resolvedTargetPath: string,
  resolvedParentDir: string,
): boolean {
  const rel = path.relative(resolvedParentDir, resolvedTargetPath);
  // Only ".." itself and "../..." leave `resolvedParentDir`; a file named "..foo" stays inside it.
  // An absolute result means a different drive on Windows.
  return rel !== ".." && !rel.startsWith(`..${path.sep}`) && !path.isAbsolute(rel);
}
