import * as fs from "fs-extra";
import ignore from "ignore";
import * as path from "path";

// Excluded whatever the project's .gitignore says. `.git` holds no Dataform project
// files. A top-level `node_modules` can't be present at all here -- `compile()` rejects
// the project before copying if it finds one -- so that entry covers nested ones, which
// are likewise never part of a Dataform project.
//
// Checked independently of the `ignore` instance below, rather than seeded into it, so
// that a project's .gitignore cannot override this floor: `ignore` lets later patterns
// override earlier ones by design, so a `!node_modules` negation would otherwise
// un-ignore it.
const ALWAYS_IGNORED_NAMES = new Set([".git", "node_modules"]);

/**
 * Builds a filter for fs-extra's `copySync`, so the stateless-install copy in `compile()`
 * skips files that can't be part of the Dataform project -- most commonly a large
 * `.venv`, build-output or cache directory sitting alongside `definitions/`, whose size
 * the copy would otherwise pay for.
 *
 * Exclusions come from the project's own `.gitignore` rather than from a hardcoded list
 * of directory names: no fixed list covers every ecosystem's junk directories (`.venv`,
 * `target/`, `__pycache__/`, `vendor/`, `coverage/`, ...), whereas a project's
 * `.gitignore` already states exactly what that project treats as disposable, and
 * `dataform init` writes one.
 *
 * Only the project root's `.gitignore` is read. Nested `.gitignore` files,
 * `.git/info/exclude` and the user's global excludes file are not consulted, so a
 * project relying on those has more copied than `git status` would suggest. A project
 * with no `.gitignore` at all gets only the ALWAYS_IGNORED_NAMES floor.
 *
 * Note that a gitignored file is never copied, so it is also never compiled: a project
 * that generates definitions into a gitignored path needs that path unignored.
 */
export function buildProjectCopyFilter(resolvedProjectPath: string): (src: string) => boolean {
  const ig = ignore();
  const gitignorePath = path.join(resolvedProjectPath, ".gitignore");
  if (fs.existsSync(gitignorePath)) {
    ig.add(fs.readFileSync(gitignorePath, "utf8"));
  }

  return (src: string) => {
    const relative = path.relative(resolvedProjectPath, src);
    // The project root itself (relative === ""), or something outside the project
    // root (shouldn't happen in practice for a copySync(resolvedProjectPath, ...)
    // call, but not this function's place to decide) is always copied/recursed into.
    if (
      !relative ||
      relative === ".." ||
      relative.startsWith(`..${path.sep}`) ||
      path.isAbsolute(relative)
    ) {
      return true;
    }

    const relativeSegments = relative.split(path.sep);
    if (relativeSegments.some(segment => ALWAYS_IGNORED_NAMES.has(segment))) {
      return false;
    }

    // `ignore` needs to know whether a path is a directory to correctly match
    // patterns like `.venv/` (trailing slash = directories only), and fs-extra's
    // copySync filter callback isn't given that -- only `src`. Use lstatSync so
    // dangling symlinks remain copyable, matching copySync's default behavior of
    // copying links rather than dereferencing them.
    let posixRelative = relativeSegments.join("/");
    if (fs.lstatSync(src).isDirectory()) {
      posixRelative += "/";
    }

    return !ig.ignores(posixRelative);
  };
}
