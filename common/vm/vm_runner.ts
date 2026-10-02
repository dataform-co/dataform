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
import { builtinModules as nodeBuiltins } from "module";
import * as path from "path";
import * as vm from "vm";

import { ModuleResolver } from "df/common/vm/module_resolver";

export type CompilerFunction = (code: string, filePath: string) => string;

export interface VmRunnerOptions {
  projectDir: string;
  /**
   * Extensions whose files go through `compiler`; also the probe order for extension-less
   * requires (followed by `.js` and `.json`). Defaults to `["js", "json"]`.
   */
  sourceExtensions?: string[];
  compiler?: CompilerFunction;
  sandbox?: Record<string, any>;
  builtinModules?: string[];
  mockModules?: Record<string, any>;
  console?: "inherit" | "off";
  /**
   * Explicit map of environment variables exposed inside the VM context as `process.env`.
   * Mutually exclusive with `envAllowlist`.
   *
   * When neither `env` nor `envAllowlist` is set, `process.env` inside the VM is empty.
   * The CLI (`cli/vm/compile.ts`) relies on that default
   * so project code never sees host credentials or API keys.
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
  private readonly sourceExtensions: Set<string>;
  private readonly compiler?: CompilerFunction;
  private readonly builtinModules: Set<string>;
  private readonly mockModules: Record<string, any>;
  private readonly resolver: ModuleResolver;
  private readonly context: vm.Context;
  private readonly moduleCache = new Map<
    string,
    { exports: any; id: string; filename: string; loaded: boolean }
  >();
  private readonly nodeBuiltinSet: Set<string>;

  constructor(options: VmRunnerOptions) {
    this.projectDir = getRealPath(options.projectDir);
    const rawExtensions = options.sourceExtensions || ["js", "json"];
    this.sourceExtensions = new Set(
      rawExtensions.map((ext) =>
        ext.startsWith(".") ? ext.slice(1).toLowerCase() : ext.toLowerCase(),
      ),
    );
    const allExtensions = Array.from(
      new Set([
        ...rawExtensions.map((ext) => (ext.startsWith(".") ? ext : `.${ext}`)),
        ".js",
        ".json",
      ]),
    );
    this.resolver = new ModuleResolver({
      projectDir: this.projectDir,
      extensions: allExtensions,
      allowedExternalPaths: options.allowedExternalPaths,
      allowedModules: options.allowedModules,
    });
    this.compiler = options.compiler;
    this.builtinModules = new Set(
      options.builtinModules !== undefined ? options.builtinModules : ["path"],
    );
    this.mockModules = options.mockModules || {};
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
    return this.resolver.resolve(moduleName, fromPath);
  }

  private executeModule(source: string, filename: string, isRunEntryPoint: boolean = false): any {
    // Code passed to run() is synthetic and not the contents of `filename`, so it must not be
    // cached: otherwise a real file at that path (e.g. <projectDir>/index.js) would resolve to the
    // entry point's exports.
    const shouldCache = !isRunEntryPoint;

    const ext = path.extname(filename).toLowerCase().replace(/^\./, "");
    if (ext === "json") {
      const jsonModule = {
        exports: parseJson(source, filename),
        id: filename,
        filename,
        loaded: true,
      };
      if (shouldCache) {
        this.moduleCache.set(filename, jsonModule);
      }
      return jsonModule.exports;
    }

    let code = source;
    if (this.compiler && this.sourceExtensions.has(ext)) {
      code = this.compiler(code, filename);
    }

    // Required modules run in strict mode; the entry script passed to run() does not.
    const strict = !isRunEntryPoint;
    if (strict) {
      code = `"use strict";\n${code}`;
    }

    const fn = vm.compileFunction(
      code,
      ["exports", "require", "module", "__filename", "__dirname"],
      {
        filename,
        lineOffset: strict ? -1 : 0,
        parsingContext: this.context,
      },
    );

    const module = {
      exports: {},
      id: filename,
      filename,
      loaded: false,
    };
    if (shouldCache) {
      this.moduleCache.set(filename, module);
    }
    const initialExports = module.exports;

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

      if (!isRunEntryPoint) {
        return module.exports;
      }
      // run() returns the script's return value. A script without one (or returning `undefined`)
      // that populated module.exports returns those exports instead; a script that did neither
      // returns undefined rather than the untouched empty exports object.
      if (result !== undefined) {
        return result;
      }
      return hasExports(module.exports, initialExports) ? module.exports : undefined;
    } catch (e) {
      if (shouldCache) {
        this.moduleCache.delete(filename);
      }
      throw e;
    }
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

/** Parses a JSON module, prefixing errors with the file path the same way Node's loader does. */
function parseJson(source: string, filename: string): any {
  try {
    return JSON.parse(source);
  } catch (e) {
    e.message = `${filename}: ${e.message}`;
    throw e;
  }
}

/** True if a script reassigned `module.exports` or added anything to the original object. */
function hasExports(exports: any, initialExports: object): boolean {
  return exports !== initialExports || Reflect.ownKeys(initialExports).length > 0;
}

function getRealPath(targetPath: string): string {
  try {
    return fs.realpathSync(targetPath);
  } catch {
    return path.resolve(targetPath);
  }
}
