import * as fs from "fs";
import * as glob from "glob";
import * as path from "path";
import * as semver from "semver";
import { CompilerFunction, NodeVM } from "vm2";

import { encode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";

export function compile(compileConfig: dataform.ICompileConfig) {
  compileConfig.projectDir = fs.realpathSync(path.resolve(compileConfig.projectDir));
  const coreBundlePath = path.join(
    compileConfig.projectDir, "node_modules", "@dataform", "core", "bundle.js"
  );
  if (!fs.existsSync(coreBundlePath)) {
    throw new Error(
      "Could not find a recent installed version of @dataform/core in the project. Check that " +
        "either `dataformCoreVersion` is specified in `workflow_settings.yaml`, or " +
        "`@dataform/core` is specified in `package.json`. If using `package.json`, then run " +
        "`dataform install`."
    );
  }

  const vmIndexFileName = path.resolve(path.join(compileConfig.projectDir, "index.js"));

  // Retrieve compiler and version from the resolved @dataform/core. Going
  // through Node's resolver inside the vm covers every install layout
  // (package.json, workflow_settings.yaml, JiT) and matches what the user's
  // code will see. require() caches the bundle so the second call is free.
  const indexGeneratorVm = new NodeVM({
    wrapper: "none",
    require: {
      context: "sandbox",
      root: compileConfig.projectDir,
      external: true,
      builtin: ["path"]
    }
  });
  const compiler: CompilerFunction = indexGeneratorVm.run(
    'return require("@dataform/core").compiler',
    vmIndexFileName
  );
  const dataformCoreVersion: string = indexGeneratorVm.run(
    'return require("@dataform/core").version || "0.0.0"',
    vmIndexFileName
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
          `\`dataform install\`.`
      );
    }
  }
  const needsCallerFileShim = semver.lt(dataformCoreVersion, "3.0.57");

  // vm2 strips file paths from V8 CallSite objects inside the sandbox, so
  // getCallerFile() in @dataform/core needs a fallback. Track the currently
  // executing file via a host-side stack exposed through sandbox helpers, and
  // expose it as a getter on `global.__dataform_current_file`.
  const fileStack: string[] = [];

  // Then use vm2's native compiler integration to apply the compiler to files.
  const userCodeVm = new NodeVM({
    wrapper: "none",
    sandbox: {
      __df_enter: (p: string) => { fileStack.push(p); },
      __df_exit: () => { fileStack.pop(); },
      __df_current: () => fileStack.length > 0 ? fileStack[fileStack.length - 1] : null
    },
    require: {
      builtin: ["path", "fs"],
      context: "sandbox",
      external: true,
      root: compileConfig.projectDir,
      resolve: (moduleName: string, parentDirName: string) => {
        if (moduleName === "path" || moduleName === "fs") {
          return moduleName;
        }
        return path.join(parentDirName, path.relative(parentDirName, compileConfig.projectDir), moduleName);
      }
    },
    sourceExtensions: ["js", "sql", "sqlx", "yaml", "yml"],
    compiler: (code, filePath) => {
      let source = code;
      if (needsCallerFileShim && filePath === coreBundlePath) {
        source = patchOldCoreCallerFile(source);
      }
      const compiledCode = compiler(source, filePath);
      return `
        __df_enter(${JSON.stringify(filePath)});
        try {
          ${compiledCode}
        } finally {
          __df_exit();
        }
      `;
    }
  });

  const hasWorkflowSettingsYaml = fs.existsSync(
    path.join(compileConfig.projectDir, "workflow_settings.yaml")
  );
  const hasDataformJson = fs.existsSync(
    path.join(compileConfig.projectDir, "dataform.json")
  );

  return userCodeVm.run(
    `
      Object.defineProperty(global, '__dataform_current_file', {
        configurable: true,
        get: function() { return __df_current(); }
      });
      ${hasWorkflowSettingsYaml
        ? 'global.workflowSettingsYaml = require("./workflow_settings.yaml");'
        : ''}
      ${hasDataformJson
        ? 'global.dataformJson = require("./dataform.json");'
        : ''}
      return require("@dataform/core").main("${createCoreExecutionRequest(compileConfig)}")
    `,
    vmIndexFileName
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
    const pkg = JSON.parse(
      fs.readFileSync(path.join(__dirname, "package.json"), "utf8")
    );
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
  'if(!t){if(global.__dataform_current_file){t=global.__dataform_current_file}' +
  'else{throw new Error("Unable to find valid caller file; please report this issue.")}}';

function patchOldCoreCallerFile(source: string): string {
  return source.replace(OLD_CORE_THROW, OLD_CORE_WITH_FALLBACK);
}

/**
 * @returns a base64 encoded @see {@link dataform.CoreExecutionRequest} proto.
 */
function createCoreExecutionRequest(compileConfig: dataform.ICompileConfig): string {
  const filePaths = Array.from(
    new Set<string>(glob.sync("!(node_modules)/**/*.*", { cwd: compileConfig.projectDir }))
  );

  return encode64(dataform.CoreExecutionRequest, {
    // Add the list of file paths to the compile config if not already set.
    compile: { compileConfig: { filePaths, ...compileConfig } }
  });
}
