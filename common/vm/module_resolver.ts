/**
 * @fileoverview ModuleResolver maps `require()` specifiers to files for VmRunner.
 *
 * Resolution order (Node's CommonJS rules, plus a project-relative fallback):
 * - Relative and absolute specifiers resolve against the requiring file's directory.
 * - Bare specifiers go through Node's node_modules lookup first (node_modules chain from the
 *   requiring file, package.json "exports"), and only then fall back to a project-relative lookup
 *   such as `require("includes/helpers")`.
 * - For a given candidate path, a file (optionally with one of the configured extensions) wins over
 *   a directory of the same name.
 *
 * Every resolved path must be contained in the project directory (or an explicitly allowed
 * external path), so a project only ever compiles from its own directory.
 */

import * as fs from "fs";
import * as path from "path";

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
      return !rel.startsWith("..") && !path.isAbsolute(rel);
    };

    if (isContainedIn(this.projectDir)) {
      return true;
    }
    return this.allowedExternalPaths.some((allowed) => isContainedIn(allowed));
  }

  private resolveUncached(moduleName: string, fromPath: string): string {
    const parentDir = path.dirname(fromPath);

    if (
      moduleName.startsWith("./") ||
      moduleName.startsWith("../") ||
      path.isAbsolute(moduleName)
    ) {
      const resolved = this.tryResolvePath(path.resolve(parentDir, moduleName));
      if (!resolved) {
        throw moduleNotFoundError(moduleName, fromPath);
      }
      this.assertPathContained(resolved, moduleName);
      return resolved;
    }

    if (!this.isModuleAllowed(moduleName)) {
      const err: any = new Error(`Access to module '${moduleName}' is not allowed`);
      err.code = "MODULE_NOT_FOUND";
      throw err;
    }

    // Node's node_modules lookup from the requiring file. This walks the node_modules chain (so a
    // nested dependency gets its own copy) and honours package.json "exports".
    let nodeResolveError: Error | undefined;
    let containmentError: Error | undefined;
    try {
      const resolved = findInNodeModules(moduleName, parentDir);
      if (resolved) {
        if (this.isPathContained(resolved)) {
          return resolved;
        }
        // Found only outside the project (e.g. hoisted into a parent directory). Keep looking
        // inside the project, and report the containment violation if nothing else matches.
        containmentError = this.outsideProjectError(moduleName);
      }
    } catch (e) {
      // E.g. ERR_PACKAGE_PATH_NOT_EXPORTED; attached as `cause` if nothing else matches.
      nodeResolveError = e;
    }

    // Fallback: project-relative bare paths, e.g. require("includes/helpers").
    const projectRelative = this.tryResolvePath(path.resolve(this.projectDir, moduleName));
    if (projectRelative) {
      this.assertPathContained(projectRelative, moduleName);
      return projectRelative;
    }

    if (containmentError) {
      throw containmentError;
    }
    throw moduleNotFoundError(moduleName, fromPath, nodeResolveError);
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

/**
 * Looks up a bare specifier in the node_modules directories above `parentDir`, using the same
 * helpers `require.resolve()` uses internally (including package.json "exports").
 *
 * This deliberately skips `Module._resolveFilename`: hosts such as Bazel's require patch (and some
 * tests) monkeypatch it to resolve against their own runfiles, and what user code can load must
 * depend only on the project's files.
 *
 * @returns the resolved (real) path, or null if nothing was found.
 */
function findInNodeModules(moduleName: string, parentDir: string): string | null {
  const lookupPaths: string[] = NodeModule._nodeModulePaths(parentDir);
  return NodeModule._findPath(moduleName, lookupPaths, false) || null;
}

function moduleNotFoundError(moduleName: string, fromPath: string, cause?: Error): Error {
  const err: any = new Error(`Cannot find module '${moduleName}' from '${fromPath}'`);
  err.code = "MODULE_NOT_FOUND";
  if (cause) {
    err.cause = cause;
  }
  return err;
}

function getRealPath(targetPath: string): string {
  try {
    return fs.realpathSync(targetPath);
  } catch {
    return path.resolve(targetPath);
  }
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
