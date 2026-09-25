/**
 * @fileoverview VmRunner executes JavaScript code within a Node.js `node:vm` context.
 *
 * IMPORTANT SECURITY NOTE:
 * This class is NOT a security boundary or a sandbox against untrusted code.
 * As documented by Node.js, `node:vm` contexts can be escaped and do not isolate
 * against malicious code execution. VmRunner is intended solely for module isolation,
 * custom module resolution, and scoping execution environments (e.g. Dataform CLI
 * compilation and testing) where the code being executed is trusted.
 */

import * as fs from "fs";
import { builtinModules as nodeBuiltins, createRequire } from "module";
import * as path from "path";
import * as vm from "vm";

export type CompilerFunction = (code: string, filePath: string) => string;

export interface VmRunnerOptions {
  projectDir: string;
  sourceExtensions?: string[];
  compiler?: CompilerFunction;
  sandbox?: Record<string, any>;
  builtinModules?: string[];
  mockModules?: Record<string, any>;
  /**
   * Optional custom resolver hook to resolve module names before falling back
   * to standard project-relative or node_modules resolution.
   */
  customResolve?: (moduleName: string, parentDirName: string) => string;
  console?: "inherit" | "off";
  /**
   * Explicit map of environment variables exposed inside the VM context as `process.env`.
   * Mutually exclusive with `envAllowlist`.
   */
  env?: Record<string, string>;
  /**
   * Allowlist of environment variable names to copy from host `process.env` into the VM.
   * Mutually exclusive with `env`.
   */
  envAllowlist?: string[];
  allowedExternalPaths?: string[];
  allowedModules?: string[];
}

export class VmRunner {
  private readonly projectDir: string;
  private readonly allowedExternalPaths: string[];
  private readonly allowedModules?: string[];
  private readonly sourceExtensions: Set<string>;
  private readonly allExtensions: string[];
  private readonly compiler?: CompilerFunction;
  private readonly builtinModules: Set<string>;
  private readonly mockModules: Record<string, any>;
  private readonly customResolve?: (moduleName: string, parentDirName: string) => string;
  private readonly context: vm.Context;
  private readonly moduleCache = new Map<
    string,
    { exports: any; id: string; filename: string; loaded: boolean }
  >();
  private readonly resolveCache = new Map<string, string>();
  private readonly nodeBuiltinSet: Set<string>;

  constructor(options: VmRunnerOptions) {
    this.projectDir = this.getRealPath(options.projectDir);
    this.allowedExternalPaths = (options.allowedExternalPaths || []).map((p) =>
      this.getRealPath(p),
    );
    this.allowedModules = options.allowedModules;
    const rawExtensions = options.sourceExtensions || ["js", "json"];
    this.sourceExtensions = new Set(
      rawExtensions.map((ext) =>
        ext.startsWith(".") ? ext.slice(1).toLowerCase() : ext.toLowerCase(),
      ),
    );
    this.allExtensions = Array.from(
      new Set([
        ".js",
        ".json",
        ...rawExtensions.map((ext) => (ext.startsWith(".") ? ext : `.${ext}`)),
      ]),
    );
    this.compiler = options.compiler;
    this.builtinModules = new Set(
      options.builtinModules !== undefined ? options.builtinModules : ["path"],
    );
    this.mockModules = options.mockModules || {};
    this.customResolve = options.customResolve;
    this.nodeBuiltinSet = new Set(nodeBuiltins);

    if (options.env !== undefined && options.envAllowlist !== undefined) {
      throw new Error("Cannot specify both 'env' and 'envAllowlist' in VmRunnerOptions");
    }

    let env: Record<string, string | undefined>;
    if (options.env !== undefined) {
      env = { ...options.env };
    } else if (options.envAllowlist !== undefined) {
      env = {};
      for (const key of options.envAllowlist) {
        if (key in process.env) {
          env[key] = process.env[key];
        }
      }
    } else {
      // Default to empty environment to avoid leaking host secrets (credentials, API keys).
      env = {};
    }

    const hrtime = process.hrtime.bind(process) as any;
    if (typeof process.hrtime.bigint === "function") {
      hrtime.bigint = process.hrtime.bigint.bind(process.hrtime);
    }

    const sandbox: Record<string, any> = {
      console:
        options.console === "off"
          ? { log: () => {}, error: () => {}, warn: () => {}, info: () => {} }
          : console,
      process: {
        env,
        cwd: () => this.projectDir,
        version: process.version,
        versions: process.versions,
        platform: process.platform,
        arch: process.arch,
        hrtime,
        nextTick: process.nextTick.bind(process),
      },
      Buffer,
      Uint8Array,
      ArrayBuffer,
      setTimeout,
      clearTimeout,
      setInterval,
      clearInterval,
      setImmediate,
      clearImmediate,
      URL,
      URLSearchParams,
      TextEncoder,
      TextDecoder,
      ...(options.sandbox || {}),
    };

    this.context = vm.createContext(sandbox);
    sandbox.global = this.context;
    sandbox.globalThis = this.context;
  }

  public run(code: string, filename: string = path.join(this.projectDir, "index.js")): any {
    return this.executeModule(code, filename, true);
  }

  public require(
    moduleName: string,
    fromPath: string = path.join(this.projectDir, "index.js"),
  ): any {
    if (Object.prototype.hasOwnProperty.call(this.mockModules, moduleName)) {
      return this.mockModules[moduleName];
    }

    const cleanBuiltinName = moduleName.startsWith("node:") ? moduleName.slice(5) : moduleName;
    if (this.nodeBuiltinSet.has(cleanBuiltinName)) {
      if (this.builtinModules.has(cleanBuiltinName) || this.builtinModules.has(moduleName)) {
        return require(moduleName);
      }
      const err: any = new Error(`Access to built-in module '${moduleName}' is not allowed`);
      err.code = "MODULE_NOT_FOUND";
      throw err;
    }

    const resolvedPath = this.resolve(moduleName, fromPath);
    if (this.moduleCache.has(resolvedPath)) {
      return this.moduleCache.get(resolvedPath)!.exports;
    }

    const source = fs.readFileSync(resolvedPath, "utf8");
    return this.executeModule(source, resolvedPath, false);
  }

  public resolve(moduleName: string, fromPath: string): string {
    const cacheKey = `${fromPath}\0${moduleName}`;
    if (this.resolveCache.has(cacheKey)) {
      return this.resolveCache.get(cacheKey)!;
    }

    const parentDir = path.dirname(fromPath);

    // Check custom resolve function if provided
    if (this.customResolve) {
      let candidate: string | undefined;
      try {
        candidate = this.customResolve(moduleName, parentDir);
      } catch (e) {
        if (e && (e as any).code !== "MODULE_NOT_FOUND") {
          throw e;
        }
      }
      if (candidate) {
        const resolved = this.tryResolvePath(candidate);
        if (resolved) {
          this.assertPathContained(resolved, moduleName);
          this.resolveCache.set(cacheKey, resolved);
          return resolved;
        }
      }
    }

    // Relative or absolute path
    if (
      moduleName.startsWith("./") ||
      moduleName.startsWith("../") ||
      path.isAbsolute(moduleName)
    ) {
      const candidate = path.resolve(parentDir, moduleName);
      const resolved = this.tryResolvePath(candidate);
      if (resolved) {
        this.assertPathContained(resolved, moduleName);
        this.resolveCache.set(cacheKey, resolved);
        return resolved;
      }
    } else {
      // External module check: if allowedModules is specified, bare specifier must be allowed
      if (!this.isModuleAllowed(moduleName)) {
        const err: any = new Error(`Access to module '${moduleName}' is not allowed`);
        err.code = "MODULE_NOT_FOUND";
        throw err;
      }

      // Project-relative path (e.g. require("includes/helpers"))
      const projectRelative = path.resolve(this.projectDir, moduleName);
      const resolvedProjectRelative = this.tryResolvePath(projectRelative);
      if (resolvedProjectRelative) {
        this.assertPathContained(resolvedProjectRelative, moduleName);
        this.resolveCache.set(cacheKey, resolvedProjectRelative);
        return resolvedProjectRelative;
      }

      // Check project node_modules directory directly (e.g. @dataform/core)
      const nodeModulesCandidate = path.resolve(this.projectDir, "node_modules", moduleName);
      const resolvedNodeModules = this.tryResolvePath(nodeModulesCandidate);
      if (resolvedNodeModules) {
        this.assertPathContained(resolvedNodeModules, moduleName);
        this.resolveCache.set(cacheKey, resolvedNodeModules);
        return resolvedNodeModules;
      }

      // Fallback to standard Node.js require.resolve resolution
      let nodeReqResolved: string | undefined;
      let nodeReqError: any;
      try {
        const nodeReq = createRequire(fromPath);
        nodeReqResolved = nodeReq.resolve(moduleName);
      } catch (e) {
        nodeReqError = e;
      }
      if (nodeReqResolved) {
        this.assertPathContained(nodeReqResolved, moduleName);
        this.resolveCache.set(cacheKey, nodeReqResolved);
        return nodeReqResolved;
      }

      let projectReqResolved: string | undefined;
      let projectReqError: any;
      try {
        const projectReq = createRequire(path.join(this.projectDir, "index.js"));
        projectReqResolved = projectReq.resolve(moduleName);
      } catch (e) {
        projectReqError = e;
      }
      if (projectReqResolved) {
        this.assertPathContained(projectReqResolved, moduleName);
        this.resolveCache.set(cacheKey, projectReqResolved);
        return projectReqResolved;
      }

      const err: any = new Error(`Cannot find module '${moduleName}' from '${fromPath}'`);
      err.code = "MODULE_NOT_FOUND";
      if (nodeReqError || projectReqError) {
        err.cause = nodeReqError || projectReqError;
      }
      throw err;
    }

    const err: any = new Error(`Cannot find module '${moduleName}' from '${fromPath}'`);
    err.code = "MODULE_NOT_FOUND";
    throw err;
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
      const err: any = new Error(
        `Cannot require '${moduleName}' outside of project directory '${this.projectDir}'`,
      );
      err.code = "MODULE_NOT_FOUND";
      throw err;
    }
  }

  private executeModule(source: string, filename: string, isRunEntryPoint: boolean = false): any {
    const ext = path.extname(filename).toLowerCase().replace(/^\./, "");
    if (ext === "json") {
      const module = {
        exports: JSON.parse(source),
        id: filename,
        filename,
        loaded: true,
      };
      this.moduleCache.set(filename, module);
      return module.exports;
    }

    let code = source;
    if (this.compiler && this.sourceExtensions.has(ext)) {
      code = this.compiler(code, filename);
    }

    const fn = vm.compileFunction(
      code,
      ["exports", "require", "module", "__filename", "__dirname"],
      {
        filename,
        parsingContext: this.context,
      },
    );

    const module = {
      exports: {},
      id: filename,
      filename,
      loaded: false,
    };
    this.moduleCache.set(filename, module);

    try {
      const scopedRequire = this.createRequire(filename);
      const result = fn.call(
        module.exports,
        module.exports,
        scopedRequire,
        module,
        filename,
        path.dirname(filename),
      );
      module.loaded = true;

      return isRunEntryPoint && result !== undefined ? result : module.exports;
    } catch (e) {
      this.moduleCache.delete(filename);
      throw e;
    }
  }

  private getRealPath(targetPath: string): string {
    try {
      return fs.realpathSync(targetPath);
    } catch {
      return path.resolve(targetPath);
    }
  }

  private isPathContained(targetPath: string): boolean {
    const realTarget = this.getRealPath(targetPath);
    const isContainedIn = (parentDir: string) => {
      const rel = path.relative(parentDir, realTarget);
      return !rel.startsWith("..") && !path.isAbsolute(rel);
    };

    if (isContainedIn(this.projectDir)) {
      return true;
    }
    return this.allowedExternalPaths.some((allowed) => isContainedIn(allowed));
  }

  private getStat(targetPath: string): fs.Stats | null {
    try {
      return fs.statSync(targetPath);
    } catch {
      return null;
    }
  }

  private tryResolvePath(
    candidatePath: string,
    visitedDirs: Set<string> = new Set<string>(),
  ): string | null {
    const stat = this.getStat(candidatePath);
    if (stat) {
      if (stat.isFile()) {
        return candidatePath;
      }
      if (stat.isDirectory()) {
        if (visitedDirs.has(candidatePath)) {
          return null;
        }
        visitedDirs.add(candidatePath);

        const pkgPath = path.join(candidatePath, "package.json");
        const pkgStat = this.getStat(pkgPath);
        if (pkgStat && pkgStat.isFile()) {
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
          } catch {}
        }
        for (const ext of this.allExtensions) {
          const indexPath = path.join(candidatePath, `index${ext}`);
          const indexStat = this.getStat(indexPath);
          if (indexStat && indexStat.isFile()) {
            return indexPath;
          }
        }
      }
    }

    for (const ext of this.allExtensions) {
      const withExt = candidatePath.endsWith(ext) ? candidatePath : `${candidatePath}${ext}`;
      const withExtStat = this.getStat(withExt);
      if (withExtStat && withExtStat.isFile()) {
        return withExt;
      }
    }

    return null;
  }

  private createRequire(fromPath: string): NodeJS.Require {
    const requireFn = ((moduleName: string) => {
      return this.require(moduleName, fromPath);
    }) as any;

    requireFn.resolve = (moduleName: string) => {
      return this.resolve(moduleName, fromPath);
    };
    requireFn.extensions = {};
    requireFn.main = undefined;

    return requireFn;
  }
}
