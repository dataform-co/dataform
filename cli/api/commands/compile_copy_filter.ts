import * as fs from "fs-extra";
import ignore from "ignore";
import * as path from "path";

// Excluded whatever the project's ignore files say. `.git` holds no Dataform project
// files. A top-level `node_modules` can't be present at all here -- `compile()` rejects
// the project before copying if it finds one -- so that entry covers nested ones, which
// are likewise never part of a Dataform project.
//
// Checked independently of the `ignore` instance below, rather than seeded into it, so
// that a project's ignore files cannot override this floor: `ignore` lets later patterns
// override earlier ones by design, so a `!node_modules` negation would otherwise
// un-ignore it.
const ALWAYS_IGNORED_NAMES = new Set([".git", "node_modules"]);

// Project-root files that are always copied, whatever the project's ignore files say.
// `compile()` has already read `workflow_settings.yaml` from the original project to
// decide on a stateless install, and compilation in the copy can't proceed without it,
// so a broad pattern like `*.yaml` must not drop it.
const ALWAYS_COPIED_ROOT_FILES = new Set(["workflow_settings.yaml"]);

// Ignore files read from the project root, in this order. Later patterns override earlier
// ones, so a `!pattern` in `.dataformignore` can un-ignore a path the `.gitignore` excludes
// (for example, definitions generated into a gitignored directory).
export const PROJECT_IGNORE_FILE_NAMES = [".gitignore", ".dataformignore"];

/**
 * Returns the names of the PROJECT_IGNORE_FILE_NAMES present in the project root as
 * files, in the order they are applied. Anything else by that name, such as a directory,
 * is not an ignore file and is skipped rather than failing the compile.
 */
export function findProjectIgnoreFiles(resolvedProjectPath: string): string[] {
  return PROJECT_IGNORE_FILE_NAMES.filter((name) => {
    const ignoreFilePath = path.join(resolvedProjectPath, name);
    return fs.existsSync(ignoreFilePath) && fs.statSync(ignoreFilePath).isFile();
  });
}

function swapCase(name: string): string {
  return name
    .split("")
    .map((c) => (c === c.toUpperCase() ? c.toLowerCase() : c.toUpperCase()))
    .join("");
}

/**
 * Returns whether names in `directory` are matched case-insensitively by its filesystem,
 * as on default Windows and macOS volumes. It probes an existing entry under its
 * case-swapped name: if that resolves but isn't itself listed, it's the same entry.
 * Falls back to the platform default when no entry has a name with letters.
 */
export function isCaseInsensitiveDirectory(directory: string): boolean {
  const entries = fs.readdirSync(directory);
  for (const entry of entries) {
    const swapped = swapCase(entry);
    if (swapped !== entry) {
      return !entries.includes(swapped) && fs.existsSync(path.join(directory, swapped));
    }
  }
  return process.platform === "win32" || process.platform === "darwin";
}

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
 * `dataform init` writes one. An optional `.dataformignore`, in the same syntax, is
 * applied on top of it: it can exclude further paths, or un-ignore gitignored ones with
 * `!pattern`.
 *
 * Only ignore files in the project root are read. Nested `.gitignore` files,
 * `.git/info/exclude` and the user's global excludes file are not consulted, so a
 * project relying on those has more copied than `git status` would suggest. A project
 * with neither file gets only the ALWAYS_IGNORED_NAMES floor.
 *
 * Note that an ignored file is never copied, so it is also never compiled: a project
 * that generates definitions into a gitignored path needs that path unignored, in
 * either file. As in git, a file can't be re-included while any ancestor directory is
 * still excluded -- the copy never descends into that directory -- so each excluded
 * ancestor must be un-ignored too. With `definitions/generated/` in `.gitignore`,
 * `!definitions/generated/gen.sqlx` alone has no effect; `!definitions/generated/`
 * restores the directory.
 *
 * Names are matched with the case sensitivity of the project's filesystem, as git does
 * with `core.ignorecase`: on a case-sensitive one, `definitions/staging/` doesn't match
 * `definitions/Staging/`, and `NODE_MODULES` is an ordinary directory; on a
 * case-insensitive one, both patterns and negations match regardless of case. That
 * applies to the ALWAYS_IGNORED_NAMES and ALWAYS_COPIED_ROOT_FILES checks too.
 * `caseInsensitive` is detected from the project directory unless given.
 *
 * Patterns are evaluated on their own, without consulting git's index, so a file git
 * still tracks despite matching a pattern (for example, one force-added with
 * `git add -f`) is excluded all the same. The exception is ALWAYS_COPIED_ROOT_FILES.
 */
export function buildProjectCopyFilter(
  resolvedProjectPath: string,
  caseInsensitive = isCaseInsensitiveDirectory(resolvedProjectPath),
): (src: string) => boolean {
  const normalizeCase = (name: string) => (caseInsensitive ? name.toLowerCase() : name);
  const ig = ignore({ ignorecase: caseInsensitive });
  for (const name of findProjectIgnoreFiles(resolvedProjectPath)) {
    ig.add(fs.readFileSync(path.join(resolvedProjectPath, name), "utf8"));
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

    if (ALWAYS_COPIED_ROOT_FILES.has(normalizeCase(relative))) {
      return true;
    }

    const relativeSegments = relative.split(path.sep);
    if (relativeSegments.some((segment) => ALWAYS_IGNORED_NAMES.has(normalizeCase(segment)))) {
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
