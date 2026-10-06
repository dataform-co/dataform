/**
 * @fileoverview ModuleResolver maps `require()` specifiers to files for VmRunner.
 *
 * Resolution order (Node's CommonJS rules, plus a project-relative fallback):
 * - Relative and absolute specifiers resolve against the requiring file's directory. Relative
 *   specifiers may use Windows separators (`.\\helpers`) on any platform.
 * - Bare specifiers go through Node's node_modules lookup first (node_modules chain from the
 *   requiring file, package.json "exports"), and only then fall back to a project-relative lookup
 *   such as `require("includes/helpers")`. Only node_modules directories inside the project (or an
 *   allowed external path) are searched.
 * - For a given candidate path, a file (optionally with one of the configured extensions) wins over
 *   a directory of the same name.
 *
 * Every resolved path must be contained in the project directory (or an explicitly allowed
 * external path), so a project only ever compiles from its own directory.
 */

import * as fs from "fs";
import * as path from "path";

import { getRealPath } from "df/common/vm/path_utils";

// Node's CommonJS loader. Its lookup helpers are used directly (see findInNodeModules).
// tslint:disable-next-line: no-require-imports
const NodeModule: any = require("module");

export interface ModuleResolverOptions {
  projectDir: string;
  /** Extensions tried when loading a file or a directory index, e.g. [".js", ".json", ".sqlx"]. */
  extensions: string[];
  /** Directories outside `projectDir` that modules may still be loaded from. */
  allowedExternalPaths?: string[];
  /**
   * If set, bare specifiers must match one of these patterns (exact name, or `scope/*` prefix).
   * Relative and absolute specifiers are not affected.
   */
  allowedModules?: string[];
}

export class ModuleResolver {
  private readonly projectDir: string;
  private readonly extensions: string[];
  private readonly allowedExternalPaths: string[];
  private readonly allowedModules?: string[];
  private readonly resolveCache = new Map<string, string>();

  constructor(options: ModuleResolverOptions) {
    this.projectDir = getRealPath(options.projectDir);
    this.extensions = options.extensions;
    this.allowedExternalPaths = (options.allowedExternalPaths || []).map(getRealPath);
    this.allowedModules = options.allowedModules;
  }

  public resolve(moduleName: string, fromPath: string): string {
    const cacheKey = `${fromPath}\0${moduleName}`;
    const cached = this.resolveCache.get(cacheKey);
    if (cached !== undefined) {
      return cached;
    }
    const resolved = this.resolveUncached(moduleName, fromPath);
    this.resolveCache.set(cacheKey, resolved);
    return resolved;
  }

  public isPathContained(targetPath: string): boolean {
    const realTarget = getRealPath(targetPath);
    const isContainedIn = (parentDir: string) => {
      const rel = path.relative(parentDir, realTarget);
      // Only ".." itself and "../…" leave parentDir; a file named "..foo" is still inside it.
      // An absolute result means a different drive on Windows.
      return rel !== ".." && !rel.startsWith(`..${path.sep}`) && !path.isAbsolute(rel);
    };

    if (isContainedIn(this.projectDir)) {
      return true;
    }
    return this.allowedExternalPaths.some((allowed) => isContainedIn(allowed));
  }

  private resolveUncached(moduleName: string, fromPath: string): string {
    const parentDir = path.dirname(fromPath);
    // Windows-style relative specifiers (".\\helpers", "..\\utils") behave like "./helpers" and
    // "../utils" on every platform.
    const normalizedName = moduleName.replace(/\\/g, "/");

    if (
      normalizedName.startsWith("./") ||
      normalizedName.startsWith("../") ||
      path.isAbsolute(normalizedName)
    ) {
      const resolved = this.tryResolvePath(path.resolve(parentDir, normalizedName));
      if (!resolved) {
        throw moduleNotFoundError(moduleName, fromPath);
      }
      this.assertPathContained(resolved, moduleName);
      return resolved;
    }

    // Bare specifiers are gated before any lookup, including the project-relative fallback below:
    // with `allowedModules` set, a project file such as <projectDir>/lodash.js must not stand in
    // for a package that is not allowed. Relative specifiers ("./includes/helpers") are unaffected.
    if (!this.isModuleAllowed(moduleName)) {
      const err: any = new Error(`Access to module '${moduleName}' is not allowed`);
      err.code = "MODULE_NOT_FOUND";
      throw err;
    }

    // Node's node_modules lookup from the requiring file. This walks the node_modules chain (so a
    // nested dependency gets its own copy) and honours package.json "exports".
    let resolvedInNodeModules: string | null = null;
    let nodeResolveError: Error | undefined;
    try {
      resolvedInNodeModules = this.findInNodeModules(moduleName, parentDir);
    } catch (e) {
      // E.g. ERR_PACKAGE_PATH_NOT_EXPORTED; attached as `cause` if nothing else matches.
      nodeResolveError = e;
    }
    if (resolvedInNodeModules) {
      // A node_modules entry inside the project can still be a symlink to somewhere outside it.
      this.assertPathContained(resolvedInNodeModules, moduleName);
      return resolvedInNodeModules;
    }

    // Fallback: project-relative bare paths, e.g. require("includes/helpers").
    const projectRelative = this.tryResolvePath(path.resolve(this.projectDir, normalizedName));
    if (projectRelative) {
      this.assertPathContained(projectRelative, moduleName);
      return projectRelative;
    }
    throw moduleNotFoundError(moduleName, fromPath, nodeResolveError);
  }

  /**
   * Looks up a bare specifier in the node_modules directories above `parentDir`, using the same
   * helpers `require.resolve()` uses internally (including package.json "exports").
   *
   * `Module._nodeModulePaths` walks all the way up to `/node_modules`, so the candidates are
   * limited to directories inside the project (or an allowed external path): a package that merely
   * happens to exist higher up on the host, e.g. in `~/node_modules`, is never picked up and
   * resolution fails with "Cannot find module" like it would in an empty project.
   *
   * This deliberately skips `Module._resolveFilename`: hosts such as Bazel's require patch (and some
   * tests) monkeypatch it to resolve against their own runfiles, and what user code can load must
   * depend only on the project's files.
   *
   * @returns the resolved (real) path, or null if nothing was found.
   */
  private findInNodeModules(moduleName: string, parentDir: string): string | null {
    const lookupPaths = (NodeModule._nodeModulePaths(parentDir) as string[]).filter((lookupPath) =>
      this.isPathContained(lookupPath),
    );
    return NodeModule._findPath(moduleName, lookupPaths, false) || null;
  }

  private isModuleAllowed(moduleName: string): boolean {
    if (!this.allowedModules) {
      return true;
    }
    return this.allowedModules.some((pattern) => {
      if (pattern.endsWith("/*")) {
        const prefix = pattern.slice(0, -1);
        return moduleName.startsWith(prefix);
      }
      return moduleName === pattern;
    });
  }

  private assertPathContained(resolvedPath: string, moduleName: string): void {
    if (!this.isPathContained(resolvedPath)) {
      throw this.outsideProjectError(moduleName);
    }
  }

  private outsideProjectError(moduleName: string): Error {
    const err: any = new Error(
      `Cannot require '${moduleName}' outside of project directory '${this.projectDir}'`,
    );
    err.code = "MODULE_NOT_FOUND";
    return err;
  }

  /** Tries the candidate as a file first, then as a directory (same order as Node). */
  private tryResolvePath(
    candidatePath: string,
    visitedDirs: Set<string> = new Set<string>(),
  ): string | null {
    return this.tryFile(candidatePath) || this.tryDirectory(candidatePath, visitedDirs);
  }

  private tryFile(candidatePath: string): string | null {
    if (isFile(candidatePath)) {
      return candidatePath;
    }
    for (const ext of this.extensions) {
      const withExt = `${candidatePath}${ext}`;
      if (isFile(withExt)) {
        return withExt;
      }
    }
    return null;
  }

  private tryDirectory(candidatePath: string, visitedDirs: Set<string>): string | null {
    const stat = getStat(candidatePath);
    if (!stat || !stat.isDirectory() || visitedDirs.has(candidatePath)) {
      return null;
    }
    visitedDirs.add(candidatePath);

    const pkgPath = path.join(candidatePath, "package.json");
    if (isFile(pkgPath)) {
      try {
        const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
        if (pkg.main && typeof pkg.main === "string") {
          const mainPath = path.resolve(candidatePath, pkg.main);
          if (mainPath !== candidatePath) {
            const resolvedMain = this.tryResolvePath(mainPath, visitedDirs);
            if (resolvedMain) {
              return resolvedMain;
            }
          }
        }
      } catch {
        // A malformed package.json falls back to the directory index, like Node does for a
        // missing "main".
      }
    }

    for (const ext of this.extensions) {
      const indexPath = path.join(candidatePath, `index${ext}`);
      if (isFile(indexPath)) {
        return indexPath;
      }
    }
    return null;
  }
}

function moduleNotFoundError(moduleName: string, fromPath: string, cause?: Error): Error {
  const err: any = new Error(`Cannot find module '${moduleName}' from '${fromPath}'`);
  err.code = "MODULE_NOT_FOUND";
  if (cause) {
    err.cause = cause;
  }
  return err;
}

function getStat(targetPath: string): fs.Stats | null {
  try {
    return fs.statSync(targetPath);
  } catch {
    return null;
  }
}

function isFile(targetPath: string): boolean {
  const stat = getStat(targetPath);
  return !!stat && stat.isFile();
}
