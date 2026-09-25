import * as fs from "fs";
import * as glob from "glob";
import * as path from "path";
import * as semver from "semver";

import { encode64 } from "df/common/protos";
import { CompilerFunction, VmRunner } from "df/common/vm/vm_runner";
import { dataform } from "df/protos/ts";

export function compile(compileConfig: dataform.ICompileConfig) {
  compileConfig.projectDir = fs.realpathSync(path.resolve(compileConfig.projectDir));
  const coreBundlePath = path.join(
    compileConfig.projectDir,
    "node_modules",
    "@dataform",
    "core",
    "bundle.js",
  );
  if (!fs.existsSync(coreBundlePath)) {
    throw new Error(
      "Could not find a recent installed version of @dataform/core in the project. Check that " +
        "either `dataformCoreVersion` is specified in `workflow_settings.yaml`, or " +
        "`@dataform/core` is specified in `package.json`. If using `package.json`, then run " +
        "`dataform install`.",
    );
  }

  const vmIndexFileName = path.resolve(path.join(compileConfig.projectDir, "index.js"));

  // Retrieve compiler and version from the resolved @dataform/core. Going
  // through Node's resolver inside the vm covers every install layout
  // (package.json, workflow_settings.yaml, JiT) and matches what the user's
  // code will see. require() caches the bundle so the second call is free.
  const indexGeneratorVm = new VmRunner({
    projectDir: compileConfig.projectDir,
    builtinModules: ["path"],
  });
  const compiler: CompilerFunction = indexGeneratorVm.run(
    'return require("@dataform/core").compiler',
    vmIndexFileName,
  );
  const dataformCoreVersion: string = indexGeneratorVm.run(
    'return require("@dataform/core").version || "0.0.0"',
    vmIndexFileName,
  );

  const cliVersion = readCliVersion();
  const cliParsed = semver.parse(cliVersion);
  // cliParsed is null for unparseable strings, and "0.0.0" is the sentinel
  // returned when package.json can't be read (unbundled local dev). In both
  // cases skip the check rather than reject every real Core install.
  if (cliParsed && cliVersion !== "0.0.0") {
    const minCoreVersion = `${cliParsed.major}.${cliParsed.minor}.0`;
    if (
      semver.major(dataformCoreVersion) !== cliParsed.major ||
      semver.lt(dataformCoreVersion, minCoreVersion)
    ) {
      throw new Error(
        `@dataform/core ${dataformCoreVersion} is not compatible with @dataform/cli ` +
          `${cliVersion}. The CLI requires @dataform/core >= ${minCoreVersion} ` +
          `(matching major.minor). Set \`dataformCoreVersion: ${cliVersion}\` in ` +
          `workflow_settings.yaml (or pin @dataform/core in package.json), then run ` +
          `\`dataform install\`.`,
      );
    }
  }
  const needsCallerFileShim = semver.lt(dataformCoreVersion, "3.0.57");

  // While VmRunner preserves V8 CallSite file paths natively, older @dataform/core
  // versions check global.__dataform_current_file as a fallback. Track the currently
  // executing file via a host-side stack exposed through sandbox helpers, and
  // expose it as a getter on `global.__dataform_current_file`.
  const fileStack: string[] = [];
  const sandbox: Record<string, any> = {};
  if (needsCallerFileShim) {
    sandbox.__df_enter = (p: string) => {
      fileStack.push(p);
    };
    sandbox.__df_exit = () => {
      fileStack.pop();
    };
    sandbox.__df_current = () => (fileStack.length > 0 ? fileStack[fileStack.length - 1] : null);
  }

  // Then use VmRunner to apply the compiler to files.
  const userCodeVm = new VmRunner({
    projectDir: compileConfig.projectDir,
    sandbox,
    builtinModules: ["path"],
    sourceExtensions: ["js", "sql", "sqlx", "yaml", "yml", "ipynb", "md"],
    compiler: (code, filePath) => {
      let source = code;
      if (needsCallerFileShim && filePath === coreBundlePath) {
        source = patchOldCoreCallerFile(source);
      }
      const compiledCode = compiler(source, filePath);
      if (!needsCallerFileShim) {
        return compiledCode;
      }
      return `
        __df_enter(${JSON.stringify(filePath)});
        try {
          ${compiledCode}
        } finally {
          __df_exit();
        }
      `;
    },
  });

  const hasWorkflowSettingsYaml = fs.existsSync(
    path.join(compileConfig.projectDir, "workflow_settings.yaml"),
  );
  const hasDataformJson = fs.existsSync(path.join(compileConfig.projectDir, "dataform.json"));

  return userCodeVm.run(
    `
      ${
        needsCallerFileShim
          ? `Object.defineProperty(global, '__dataform_current_file', {
        configurable: true,
        get: function() { return __df_current(); }
      });`
          : ""
      }
      ${
        hasWorkflowSettingsYaml
          ? `global.workflowSettingsYaml = (function() {
               try { return require("./workflow_settings.yaml"); }
               catch(e) { console.error("YAML require failed run_core:", e); }
             })();`
          : ""
      }
      ${hasDataformJson ? 'global.dataformJson = require("./dataform.json");' : ""}
      return require("@dataform/core").main("${createCoreExecutionRequest(compileConfig)}")
    `,
    vmIndexFileName,
  );
}

export function listenForCompileRequest() {
  process.on("message", (compileConfig: dataform.ICompileConfig & { type?: string }) => {
    // JiT messages are handled by handleJitRequest in worker.ts; skip them here.
    if ((compileConfig as { type?: string })?.type === "jit_compile") {
      return;
    }
    try {
      const compiledResult = compile(compileConfig);
      process.send(compiledResult);
    } catch (e) {
      const serializableError = {};
      for (const prop of Object.getOwnPropertyNames(e)) {
        (serializableError as any)[prop] = e[prop];
      }
      process.send(serializableError);
    }
  });
}

if (require.main === module) {
  if (process.send) {
    process.send({ type: "worker_booted" });
  }
  listenForCompileRequest();
}

// Reads the CLI's own version from the package.json baked next to the bundle
// by pkg_json(version = DF_VERSION). Returns "0.0.0" when unreadable.
function readCliVersion(): string {
  try {
    const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, "package.json"), "utf8"));
    return pkg.version || "0.0.0";
  } catch {
    return "0.0.0";
  }
}

// @dataform/core <= 3.0.56 has no `global.__dataform_current_file` fallback in
// getCallerFile(), so paired with CLI >= 3.0.57 (which uses vm2 with path
// stripping) every action fails with "Unable to find valid caller file".
// Backport the fallback by rewriting the bundle text at load time. Gated on
// version so we never touch newer core bundles whose layout differs.
const OLD_CORE_THROW =
  'if(!t)throw new Error("Unable to find valid caller file; please report this issue.")';
const OLD_CORE_WITH_FALLBACK =
  "if(!t){if(global.__dataform_current_file){t=global.__dataform_current_file}" +
  'else{throw new Error("Unable to find valid caller file; please report this issue.")}}';

function patchOldCoreCallerFile(source: string): string {
  return source.replace(OLD_CORE_THROW, OLD_CORE_WITH_FALLBACK);
}

/**
 * @returns a base64 encoded @see {@link dataform.CoreExecutionRequest} proto.
 */
function createCoreExecutionRequest(compileConfig: dataform.ICompileConfig): string {
  const filePaths = Array.from(
    new Set<string>(glob.sync("!(node_modules)/**/*.*", { cwd: compileConfig.projectDir })),
  );

  return encode64(dataform.CoreExecutionRequest, {
    // Add the list of file paths to the compile config if not already set.
    compile: { compileConfig: { filePaths, ...compileConfig } },
  });
}
