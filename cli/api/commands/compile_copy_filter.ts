import * as fs from "fs-extra";
import ignore from "ignore";
import * as path from "path";

// Excluded whatever the project's ignore files say, unless a compilation input is
// reached through one (see COMPILATION_INPUT_ROOT_NAMES). `.git` holds no Dataform
// project files. A top-level `node_modules` can't be present at all here -- `compile()`
// rejects the project before copying if it finds one -- so that entry covers nested ones
// outside the compilation inputs.
//
// Checked independently of the `ignore` instance below, rather than seeded into it, so
// that neither ignore file can override this floor: `ignore` lets later patterns
// override earlier ones by design, so a `!node_modules` negation would otherwise
// un-ignore it.
const ALWAYS_IGNORED_NAMES = new Set([".git", "node_modules"]);

// Project-root entries that compilation reads, and that are therefore copied with
// everything reachable from them, whatever the project's ignore files say.
//
// - `definitions` and `includes` are the directories core compiles from. Compilation
//   lists their files with a glob that follows symbolic links and descends into nested
//   `node_modules`, so both are followed here too. `.git` directories are still skipped:
//   the glob skips dot-directories, so nothing in them is compiled.
// - `workflow_settings.yaml` (or the legacy `dataform.json`) has already been read from
//   the original project to decide on a stateless install, and compilation in the copy
//   reads it again, so a broad pattern like `*.yaml` must not drop it.
// - `.npmrc` configures the `npm i` run in the copy (a private registry or mirror, say),
//   and is commonly gitignored because it holds auth tokens.
//
// `package.json` and `package-lock.json` aren't needed: `compile()` rejects a
// stateless-install project that has either, and writes its own `package.json` into the
// copy.
const COMPILATION_INPUT_ROOT_NAMES = new Set([
  "definitions",
  "includes",
  "workflow_settings.yaml",
  "dataform.json",
  ".npmrc",
]);

// The default filesystems on Windows and macOS are case-insensitive.
const CASE_INSENSITIVE_PLATFORM = process.platform === "win32" || process.platform === "darwin";

// Matches the limit most platforms place on symbolic links followed in resolving one
// path, so a symlink loop ends rather than recursing forever.
const MAX_SYMLINK_HOPS = 40;

// Read in this order into one matcher, so a `.dataformignore` pattern overrides the
// `.gitignore`: it can exclude more, or re-include a gitignored path with `!pattern`
// without the project changing what git ignores.
const PROJECT_IGNORE_FILE_NAMES = [".gitignore", ".dataformignore"];

/**
 * Returns the names of the ignore files present in the project root, in the order they
 * are applied. Anything else by those names, such as a directory, is not an ignore file
 * and is skipped rather than failing the compile.
 */
export function findProjectIgnoreFiles(resolvedProjectPath: string): string[] {
  return PROJECT_IGNORE_FILE_NAMES.filter((name) => {
    const ignoreFilePath = path.join(resolvedProjectPath, name);
    return fs.existsSync(ignoreFilePath) && fs.statSync(ignoreFilePath).isFile();
  });
}

function isInsideDirectory(relative: string): boolean {
  return (
    !!relative &&
    relative !== ".." &&
    !relative.startsWith(`..${path.sep}`) &&
    !path.isAbsolute(relative)
  );
}

function lstatIfExists(entryPath: string): fs.Stats | undefined {
  try {
    return fs.lstatSync(entryPath);
  } catch (e) {
    return undefined;
  }
}

/**
 * Resolves the symbolic link at `linkPath` one path component at a time, as the operating
 * system does, calling `onEntry` with the physical path of every entry it passes through,
 * including intermediate links. A relative link resolves against the copy's own directory
 * structure, so each of those entries must be copied for the link to work there.
 * `linkPath` must itself be physical (no symbolic links in its parent directories).
 *
 * Returns the physical path the link resolves to, or undefined if it's dangling or loops.
 */
function resolveSymlink(
  linkPath: string,
  onEntry: (entryPath: string) => void,
  hops = 0,
): string | undefined {
  if (hops >= MAX_SYMLINK_HOPS) {
    return undefined;
  }
  let target: string;
  try {
    target = fs.readlinkSync(linkPath);
  } catch (e) {
    return undefined;
  }
  const root = path.isAbsolute(target) ? path.parse(target).root : "";
  let current = root || path.dirname(linkPath);
  for (const part of target.slice(root.length).split(path.sep === "\\" ? /[\\/]/ : "/")) {
    if (!part || part === ".") {
      continue;
    }
    if (part === "..") {
      current = path.dirname(current);
      continue;
    }
    const next = path.join(current, part);
    onEntry(next);
    let stats: fs.Stats;
    try {
      stats = fs.lstatSync(next);
    } catch (e) {
      return undefined;
    }
    if (stats.isSymbolicLink()) {
      const resolved = resolveSymlink(next, onEntry, hops + 1);
      if (resolved === undefined) {
        return undefined;
      }
      current = resolved;
    } else {
      current = next;
    }
  }
  return current;
}

/**
 * Returns the project-relative paths, in `/`-separated form and passed through
 * `normalizeCase`, of every entry the copy must contain for compilation to see the same
 * inputs as in the original project: everything reachable from the
 * COMPILATION_INPUT_ROOT_NAMES, following symbolic links that resolve inside the project,
 * plus every entry those links pass through and the parent directories of all of them.
 *
 * A link that resolves to the project root makes the whole project reachable, since
 * compilation's glob follows it (`definitions -> .` compiles `definitions/generated/*`),
 * so everything apart from `.git` directories is then required. A link that resolves
 * outside the project is copied as a link, and not followed: the filter only decides
 * what is copied from inside the project.
 */
function collectCompilationInputs(
  resolvedProjectPath: string,
  normalizeCase: (name: string) => string,
): Set<string> {
  const projectRoot = fs.realpathSync(resolvedProjectPath);
  const required = new Set<string>();
  const visited = new Set<string>();

  const addRequired = (entryPath: string) => {
    const relative = path.relative(projectRoot, entryPath);
    if (!isInsideDirectory(relative)) {
      return;
    }
    const segments = relative.split(path.sep);
    for (let i = 1; i <= segments.length; i++) {
      required.add(normalizeCase(segments.slice(0, i).join("/")));
    }
  };

  const visit = (entryPath: string) => {
    const relative = path.relative(projectRoot, entryPath);
    if (visited.has(entryPath) || (relative !== "" && !isInsideDirectory(relative))) {
      return;
    }
    visited.add(entryPath);
    let stats: fs.Stats;
    try {
      stats = fs.lstatSync(entryPath);
    } catch (e) {
      return;
    }
    addRequired(entryPath);
    if (stats.isSymbolicLink()) {
      const resolved = resolveSymlink(entryPath, addRequired);
      if (resolved !== undefined) {
        visit(resolved);
      }
    } else if (stats.isDirectory()) {
      for (const child of fs.readdirSync(entryPath)) {
        if (normalizeCase(child) !== ".git") {
          visit(path.join(entryPath, child));
        }
      }
    }
  };

  for (const name of fs.readdirSync(projectRoot)) {
    if (COMPILATION_INPUT_ROOT_NAMES.has(normalizeCase(name))) {
      visit(path.join(projectRoot, name));
    }
  }
  return required;
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
 * `.gitignore` already lists what it keeps out of version control, which usually
 * includes those, and `dataform init` writes one. Not everything kept out of version
 * control is disposable, so an optional `.dataformignore`, in the same syntax, is applied
 * after it, for files the copy needs that git doesn't track (generated SQL that
 * `actions.yaml` references, or a CA file `.npmrc` points to), or for
 * further exclusions. As in git, a path can't be re-included while its parent directory
 * is excluded, since the copy never descends into that directory.
 *
 * Neither ignore file nor ALWAYS_IGNORED_NAMES applies to compilation inputs: the
 * COMPILATION_INPUT_ROOT_NAMES and everything reachable from them apart from `.git`
 * directories, including through symbolic links into otherwise-ignored paths inside the
 * project. Other files are filtered, including ones project code loads with `require()`;
 * if one of those is excluded, compilation in the copy fails where the original
 * project's would succeed, or, if the code catches the error, behaves differently.
 *
 * Only the ignore files in the project root are read. Nested `.gitignore` files,
 * `.git/info/exclude` and the user's global excludes file are not consulted, so the
 * copy can differ from git's ignored-file classification in either direction: a nested
 * `!helper.js` negation un-ignores a file for git that this filter still excludes. A project
 * without either ignore file gets only the ALWAYS_IGNORED_NAMES floor.
 *
 * Names are matched case-insensitively on Windows and macOS, whose default filesystems
 * are case-insensitive, and case-sensitively elsewhere. That applies to the
 * COMPILATION_INPUT_ROOT_NAMES and ALWAYS_IGNORED_NAMES too. `caseInsensitive` overrides
 * this.
 *
 * Patterns are evaluated on their own, without consulting git's index, so a file git
 * still tracks despite matching a pattern (for example, one force-added with
 * `git add -f`) is excluded all the same.
 */
export function buildProjectCopyFilter(
  resolvedProjectPath: string,
  caseInsensitive = CASE_INSENSITIVE_PLATFORM,
): (src: string) => boolean {
  const normalizeCase = (name: string) => (caseInsensitive ? name.toLowerCase() : name);
  const ig = ignore({ ignorecase: caseInsensitive });
  for (const name of findProjectIgnoreFiles(resolvedProjectPath)) {
    ig.add(fs.readFileSync(path.join(resolvedProjectPath, name), "utf8"));
  }
  const compilationInputs = collectCompilationInputs(resolvedProjectPath, normalizeCase);

  return (src: string) => {
    const relative = path.relative(resolvedProjectPath, src);
    // The project root itself (relative === ""), or something outside the project
    // root (shouldn't happen in practice for a copySync(resolvedProjectPath, ...)
    // call, but not this function's place to decide) is always copied/recursed into.
    if (!isInsideDirectory(relative)) {
      return true;
    }

    const relativeSegments = relative.split(path.sep);
    if (compilationInputs.has(normalizeCase(relativeSegments.join("/")))) {
      return true;
    }

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

/**
 * Copies the project to `destination` for a stateless install, skipping what
 * `buildProjectCopyFilter()` excludes. Symbolic links are copied as links.
 */
export function copyProjectForStatelessInstall(
  resolvedProjectPath: string,
  destination: string,
): void {
  fs.copySync(resolvedProjectPath, destination, {
    filter: buildProjectCopyFilter(resolvedProjectPath),
  });
}

/**
 * If `message` reports a module compilation couldn't find, and part of its path is in the
 * project but missing from the stateless-install copy at `copyPath`, returns `message`
 * with a note naming that path. Otherwise returns `message` unchanged.
 *
 * The note names the shallowest such path, which is the one an ignore file excluded,
 * since `copySync` doesn't descend into an excluded directory. Starting from there means
 * an extensionless name (`require("lib/helper")`) is explained when a directory on its
 * path was excluded, but not when only the file itself was. A relative name
 * (`require("./sibling")`) is resolved against the requiring file, which the message
 * doesn't name, so it is left unexplained. So is a path through a copied symbolic link:
 * what's missing is behind the link, under a path the module name doesn't show.
 *
 * The note doesn't say which pattern to add: other patterns may match the file as well,
 * and file names can contain pattern syntax, so a single suggested negation isn't always
 * enough.
 */
export function explainExcludedModule(
  message: string,
  resolvedProjectPath: string,
  copyPath: string,
): string {
  const match = /Cannot find module '([^']+)'/.exec(message);
  if (!match || match[1].startsWith(".")) {
    return message;
  }
  const moduleName = path.isAbsolute(match[1])
    ? path.relative(copyPath, match[1])
    : path.normalize(match[1]);
  if (!isInsideDirectory(moduleName)) {
    return message;
  }

  const segments = moduleName.split(path.sep);
  for (let i = 1; i <= segments.length; i++) {
    const excluded = segments.slice(0, i);
    const copied = lstatIfExists(path.join(copyPath, ...excluded));
    if (copied?.isSymbolicLink()) {
      // Whatever is missing is behind a copied link, possibly excluded under a path this
      // can't see, so re-including anything named here wouldn't help.
      return message;
    }
    if (copied) {
      continue;
    }
    const name = segments[i - 1];
    if (
      !lstatIfExists(path.join(resolvedProjectPath, ...excluded)) ||
      ALWAYS_IGNORED_NAMES.has(CASE_INSENSITIVE_PLATFORM ? name.toLowerCase() : name)
    ) {
      // Missing from the project too, or excluded by the floor no ignore file overrides.
      return message;
    }
    return (
      `${message}. '${excluded.join("/")}' is in the project but not in the copy compiled ` +
      `for dataformCoreVersion, because .gitignore or .dataformignore excludes it. If the ` +
      `module is there, re-include it in .dataformignore`
    );
  }
  return message;
}
