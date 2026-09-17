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
  resolve?: (moduleName: string, parentDirName: string) => string;
  console?: "inherit" | "off";
  env?: Record<string, string>;
  envAllowlist?: string[];
  allowedExternalPaths?: string[];
}

export class VmRunner {
  private readonly projectDir: string;
  private readonly allowedExternalPaths: string[];
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
    this.customResolve = options.resolve;
    this.nodeBuiltinSet = new Set(nodeBuiltins);

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
      env = { ...process.env };
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
    let source = code;
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

    if (this.compiler && this.sourceExtensions.has(ext)) {
      source = this.compiler(source, filename);
    }

    const fn = vm.compileFunction(
      source,
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

      return result !== undefined ? result : module.exports;
    } catch (e) {
      this.moduleCache.delete(filename);
      throw e;
    }
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

    const module = {
      exports: {},
      id: resolvedPath,
      filename: resolvedPath,
      loaded: false,
    };
    this.moduleCache.set(resolvedPath, module);

    try {
      const ext = path.extname(resolvedPath).toLowerCase().replace(/^\./, "");
      if (ext === "json") {
        const content = fs.readFileSync(resolvedPath, "utf8");
        module.exports = JSON.parse(content);
        module.loaded = true;
        return module.exports;
      }

      let source = fs.readFileSync(resolvedPath, "utf8");
      if (this.compiler && this.sourceExtensions.has(ext)) {
        source = this.compiler(source, resolvedPath);
      }

      const fn = vm.compileFunction(
        source,
        ["exports", "require", "module", "__filename", "__dirname"],
        {
          filename: resolvedPath,
          parsingContext: this.context,
        },
      );

      const scopedRequire = this.createRequire(resolvedPath);
      fn.call(
        module.exports,
        module.exports,
        scopedRequire,
        module,
        resolvedPath,
        path.dirname(resolvedPath),
      );
      module.loaded = true;

      return module.exports;
    } catch (e) {
      this.moduleCache.delete(resolvedPath);
      throw e;
    }
  }

  public resolve(moduleName: string, fromPath: string): string {
    const cacheKey = `${fromPath}\0${moduleName}`;
    if (this.resolveCache.has(cacheKey)) {
      return this.resolveCache.get(cacheKey)!;
    }

    const parentDir = path.dirname(fromPath);

    // Check custom resolve function if provided
    if (this.customResolve) {
      try {
        const candidate = this.customResolve(moduleName, parentDir);
        const resolved = this.tryResolvePath(candidate);
        if (resolved) {
          this.resolveCache.set(cacheKey, resolved);
          return resolved;
        }
      } catch {}
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
        if (!this.isPathContained(resolved)) {
          const err: any = new Error(
            `Cannot require '${moduleName}' outside of project directory '${this.projectDir}'`,
          );
          err.code = "MODULE_NOT_FOUND";
          throw err;
        }
        this.resolveCache.set(cacheKey, resolved);
        return resolved;
      }
    } else {
      // Project-relative path (e.g. require("includes/helpers"))
      const projectRelative = path.resolve(this.projectDir, moduleName);
      const resolvedProjectRelative = this.tryResolvePath(projectRelative);
      if (resolvedProjectRelative) {
        if (!this.isPathContained(resolvedProjectRelative)) {
          const err: any = new Error(
            `Cannot require '${moduleName}' outside of project directory '${this.projectDir}'`,
          );
          err.code = "MODULE_NOT_FOUND";
          throw err;
        }
        this.resolveCache.set(cacheKey, resolvedProjectRelative);
        return resolvedProjectRelative;
      }

      // Check project node_modules directory directly (e.g. @dataform/core)
      const nodeModulesCandidate = path.resolve(this.projectDir, "node_modules", moduleName);
      const resolvedNodeModules = this.tryResolvePath(nodeModulesCandidate);
      if (resolvedNodeModules) {
        this.resolveCache.set(cacheKey, resolvedNodeModules);
        return resolvedNodeModules;
      }

      // Fallback to standard Node.js require.resolve resolution
      let nodeReqError: any;
      try {
        const nodeReq = createRequire(fromPath);
        const resolved = nodeReq.resolve(moduleName);
        this.resolveCache.set(cacheKey, resolved);
        return resolved;
      } catch (e) {
        nodeReqError = e;
      }

      let projectReqError: any;
      try {
        const projectReq = createRequire(path.join(this.projectDir, "index.js"));
        const resolved = projectReq.resolve(moduleName);
        this.resolveCache.set(cacheKey, resolved);
        return resolved;
      } catch (e) {
        projectReqError = e;
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

  private tryResolvePath(candidatePath: string): string | null {
    const stat = this.getStat(candidatePath);
    if (stat) {
      if (stat.isFile()) {
        return candidatePath;
      }
      if (stat.isDirectory()) {
        const pkgPath = path.join(candidatePath, "package.json");
        const pkgStat = this.getStat(pkgPath);
        if (pkgStat && pkgStat.isFile()) {
          try {
            const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
            if (pkg.main) {
              const mainPath = path.resolve(candidatePath, pkg.main);
              const resolvedMain = this.tryResolvePath(mainPath);
              if (resolvedMain) {
                return resolvedMain;
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
