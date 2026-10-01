/**
 * End-to-end smoke tests for the Dataform CLI on Windows.
 *
 * The CLI is expected to be installed globally (`npm install -g <cli tarball>`). Each flow is
 * executed through the shell selected with `--shell` so that the `dataform.cmd` shim is exercised
 * the same way a user would run it:
 *
 *   node smoke_test_bundle.js --shell powershell [--core-tarball <path>] [--dataform-bin <path>]
 *   node smoke_test_bundle.js --shell cmd        [--core-tarball <path>] [--dataform-bin <path>]
 *
 * Without `--shell` the CLI is spawned directly, which also works on Linux and macOS.
 *
 * `--core-tarball` installs `@dataform/core` from a local tarball through `package.json`
 * (`"file:<tarball>"`), the same flow used by the CLI unit tests. Without it the version written by
 * `dataform init` is installed from the npm registry.
 *
 * This file is bundled with Bazel (`//cli/tests/windows:smoke_test_bundle`) into a single
 * CommonJS file that only depends on Node built-ins, so the Windows CI runner needs nothing but
 * Node.js to execute it.
 */

import { spawn, spawnSync, SpawnSyncReturns } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

type ShellMode = "cmd" | "powershell" | "direct";

interface IDataformBinary {
  /** `dataform.cmd` shim on Windows or the `dataform` launcher elsewhere; null if unknown. */
  shimPath: string | null;
  /** `node_modules/@dataform/cli/bundle.js`; used to spawn the CLI without any shell. */
  bundlePath: string | null;
}

interface IExecOptions {
  cwd?: string;
  timeoutMillis?: number;
}

const WATCH_TIMEOUT_MILLIS = 45_000;
const DEFAULT_TIMEOUT_MILLIS = 120_000;
const SAMPLE_TABLE_SQLX = 'config {\n  type: "table"\n}\n\nSELECT\n  1 AS sample_col\n';

// ---------------------------------------------------------------------------
// Command line flags
// ---------------------------------------------------------------------------

const argv = process.argv.slice(2);

function getFlag(flag: string): string | null {
  const index = argv.indexOf(flag);
  return index !== -1 && index + 1 < argv.length ? argv[index + 1] : null;
}

function parseShellMode(value: string | null): ShellMode {
  switch (value) {
    case null:
    case "direct":
      return "direct";
    case "cmd":
    case "powershell":
      if (os.platform() !== "win32") {
        throw new Error(`--shell ${value} is only supported on Windows`);
      }
      return value;
    default:
      throw new Error(`Unknown --shell value "${value}"; expected cmd, powershell or direct`);
  }
}

const shellMode = parseShellMode(getFlag("--shell"));
const customBinary = getFlag("--dataform-bin") || process.env.DATAFORM_BIN || null;
const coreTarball = getFlag("--core-tarball");

// ---------------------------------------------------------------------------
// Minimal assertion helpers (keeps the bundle free of non built-in dependencies)
// ---------------------------------------------------------------------------

function check(condition: boolean, message: string): void {
  if (!condition) {
    throw new Error(message);
  }
}

function checkEqual<T>(actual: T, expected: T, message: string): void {
  check(actual === expected, `${message}\n  expected: ${expected}\n  actual:   ${actual}`);
}

function checkIncludes(haystack: string, needle: string, message: string): void {
  check(haystack.includes(needle), `${message}\n  expected to find: ${needle}\n  in:\n${haystack}`);
}

function describeResult(result: SpawnSyncReturns<string>): string {
  return `exit=${result.status} signal=${result.signal}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`;
}

// ---------------------------------------------------------------------------
// Locating and invoking the CLI
// ---------------------------------------------------------------------------

function findOnPath(executable: string): string | null {
  for (const dir of (process.env.PATH || "").split(path.delimiter)) {
    const candidate = path.join(dir, executable);
    if (dir && fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

function findBundleNextToShim(shimPath: string): string | null {
  const shimDir = path.dirname(shimPath);
  const candidates = [
    // npm global prefix on Windows: %APPDATA%\npm\dataform.cmd + %APPDATA%\npm\node_modules\...
    path.join(shimDir, "node_modules", "@dataform", "cli", "bundle.js"),
    // npm global prefix on POSIX: <prefix>/bin/dataform + <prefix>/lib/node_modules/...
    path.join(shimDir, "..", "lib", "node_modules", "@dataform", "cli", "bundle.js"),
  ];
  try {
    // On POSIX the launcher is a symlink straight to bundle.js.
    candidates.unshift(fs.realpathSync(shimPath));
  } catch (e) {
    // Ignore: the shim does not exist or is not a symlink.
  }
  const found = candidates.find(
    (candidate) => candidate.endsWith(".js") && fs.existsSync(candidate),
  );
  return found ? path.resolve(found) : null;
}

function resolveDataformBinary(): IDataformBinary {
  if (customBinary) {
    const resolved = path.resolve(customBinary);
    if (resolved.endsWith(".js")) {
      return { shimPath: null, bundlePath: resolved };
    }
    return { shimPath: resolved, bundlePath: findBundleNextToShim(resolved) };
  }
  const shimPath = findOnPath(os.platform() === "win32" ? "dataform.cmd" : "dataform");
  if (!shimPath) {
    throw new Error(
      "dataform is not on PATH; install it with `npm install -g` or pass --dataform-bin",
    );
  }
  return { shimPath, bundlePath: findBundleNextToShim(shimPath) };
}

const binary = resolveDataformBinary();

/** The program and leading arguments that start the CLI, e.g. `[dataform.cmd]` or `[node, bundle.js]`. */
function cliCommand(): string[] {
  if (binary.shimPath) {
    return [binary.shimPath];
  }
  return [process.execPath, binary.bundlePath];
}

function quoteForCmd(arg: string): string {
  // cmd.exe has no escape for an embedded double quote; the arguments used here never need one.
  check(!arg.includes('"'), `cmd.exe argument must not contain double quotes: ${arg}`);
  return /[\s&|<>^()]/.test(arg) ? `"${arg}"` : arg;
}

function quoteForPowerShell(arg: string): string {
  return `'${arg.replace(/'/g, "''")}'`;
}

function execCli(args: string[], options: IExecOptions = {}): SpawnSyncReturns<string> {
  const spawnOptions = {
    cwd: options.cwd,
    env: process.env,
    encoding: "utf8" as const,
    timeout: options.timeoutMillis || DEFAULT_TIMEOUT_MILLIS,
  };
  const command = [...cliCommand(), ...args];
  switch (shellMode) {
    case "powershell":
      return spawnSync(
        "powershell",
        [
          "-NoProfile",
          "-NonInteractive",
          "-Command",
          ["&", ...command.map(quoteForPowerShell)].join(" "),
        ],
        spawnOptions,
      );
    case "cmd":
      // The whole command line is built here and passed verbatim, so Node does not re-quote it
      // (and does not emit DEP0190, which is triggered by `shell: true` with an argument array).
      // `/s` makes cmd.exe strip only the outer pair of quotes.
      return spawnSync("cmd.exe", ["/d", "/s", "/c", `"${command.map(quoteForCmd).join(" ")}"`], {
        ...spawnOptions,
        windowsVerbatimArguments: true,
      });
    default:
      return spawnSync(command[0], command.slice(1), spawnOptions);
  }
}

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

let passed = 0;
let failed = 0;

async function runTest(name: string, fn: () => void | Promise<void>): Promise<void> {
  process.stdout.write(`TEST: ${name} ... `);
  try {
    await fn();
    console.log("PASSED");
    passed++;
  } catch (e) {
    console.log("FAILED");
    console.error(`  ${e instanceof Error ? e.stack || e.message : String(e)}`);
    failed++;
  }
}

function createTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function removeDir(dir: string): void {
  // Windows may still hold handles on recently closed files; retry a few times.
  fs.rmSync(dir, { recursive: true, force: true, maxRetries: 5 });
}

function toPosixPath(filePath: string): string {
  return filePath.replace(/\\/g, "/");
}

// ---------------------------------------------------------------------------
// CLI flows
// ---------------------------------------------------------------------------

async function runCliFlows(projectDir: string): Promise<void> {
  const definitionsDir = path.join(projectDir, "definitions");
  const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
  const sampleTablePath = path.join(definitionsDir, "sample_table.sqlx");

  await runTest("CLI Flow 1: dataform --help", () => {
    const result = execCli(["--help"]);
    checkEqual(result.status, 0, `--help must exit with 0: ${describeResult(result)}`);
    checkIncludes(result.stdout, "dataform [command]", "--help must print the usage line");
  });

  await runTest("CLI Flow 2: dataform init creates the project structure", () => {
    const result = execCli(["init", projectDir, "test-db", "us-central1"]);
    checkEqual(result.status, 0, `init must exit with 0: ${describeResult(result)}`);
    check(fs.existsSync(workflowSettingsPath), "workflow_settings.yaml must exist");
    const workflowSettings = fs.readFileSync(workflowSettingsPath, "utf8");
    checkIncludes(workflowSettings, "defaultProject: test-db", "defaultProject must be set");
    checkIncludes(workflowSettings, "defaultLocation: us-central1", "defaultLocation must be set");
    check(fs.existsSync(definitionsDir), "definitions/ must exist");
    check(fs.existsSync(path.join(projectDir, "includes")), "includes/ must exist");
  });

  await runTest("CLI Flow 3A: dataform install refuses projects with dataformCoreVersion", () => {
    const result = execCli(["install", projectDir]);
    checkEqual(result.status, 1, `install must exit with 1: ${describeResult(result)}`);
    checkIncludes(
      result.stderr,
      "No installation is needed when using workflow_settings.yaml",
      "install must explain that packages are installed at runtime",
    );
  });

  await runTest("CLI Flow 3B: dataform install installs @dataform/core from package.json", () => {
    // Switch the project to the package.json flow (the same setup as the CLI unit tests): the
    // version pinned by `dataform init` moves from workflow_settings.yaml to package.json, or is
    // replaced by the local tarball when one was provided.
    const workflowSettings = fs.readFileSync(workflowSettingsPath, "utf8");
    const versionMatch = /^dataformCoreVersion:\s*(\S+)\s*$/m.exec(workflowSettings);
    check(!!versionMatch, "workflow_settings.yaml written by init must pin dataformCoreVersion");
    fs.writeFileSync(workflowSettingsPath, workflowSettings.replace(versionMatch[0], ""));
    const coreDependency = coreTarball
      ? `file:${toPosixPath(path.resolve(coreTarball))}`
      : versionMatch[1];
    fs.writeFileSync(
      path.join(projectDir, "package.json"),
      JSON.stringify({ dependencies: { "@dataform/core": coreDependency } }, null, 2),
    );

    const result = execCli(["install", projectDir]);
    checkEqual(result.status, 0, `install must exit with 0: ${describeResult(result)}`);
    check(
      fs.existsSync(path.join(projectDir, "node_modules", "@dataform", "core", "package.json")),
      "node_modules/@dataform/core must be installed",
    );
  });

  await runTest("CLI Flow 4A: dataform compile (issue #1565)", () => {
    fs.writeFileSync(sampleTablePath, SAMPLE_TABLE_SQLX);
    const result = execCli(["compile", projectDir]);
    checkEqual(result.status, 0, `compile must exit with 0: ${describeResult(result)}`);
    checkIncludes(result.stdout, "Compiled 1 action(s).", "compile must report one action");
  });

  await runTest("CLI Flow 4B: dataform compile --json uses forward slashes (issue #510)", () => {
    const result = execCli(["compile", projectDir, "--json"]);
    checkEqual(result.status, 0, `compile --json must exit with 0: ${describeResult(result)}`);
    const graph = JSON.parse(result.stdout);
    check(Array.isArray(graph.tables), "compiled graph must contain tables");
    const table = graph.tables.find((t: any) => t.target && t.target.name === "sample_table");
    check(!!table, "sample_table must be in the compiled graph");
    checkEqual(table.fileName, "definitions/sample_table.sqlx", "fileName must use '/'");
  });

  await runTest("CLI Flow 4C: dataform compile with a notebook declared in actions.yaml", () => {
    const actionsYamlPath = path.join(definitionsDir, "actions.yaml");
    const notebookPath = path.join(definitionsDir, "sample_notebook.ipynb");
    fs.writeFileSync(
      actionsYamlPath,
      "actions:\n- notebook:\n    filename: sample_notebook.ipynb\n",
    );
    fs.writeFileSync(notebookPath, JSON.stringify({ cells: [] }));
    try {
      const result = execCli(["compile", projectDir, "--json"]);
      checkEqual(result.status, 0, `compile must exit with 0: ${describeResult(result)}`);
      const graph = JSON.parse(result.stdout);
      check(Array.isArray(graph.notebooks), "compiled graph must contain notebooks");
      const notebook = graph.notebooks.find(
        (n: any) => n.target && n.target.name === "sample_notebook",
      );
      check(!!notebook, "sample_notebook must be in the compiled graph");
      checkEqual(notebook.fileName, "definitions/sample_notebook.ipynb", "fileName must use '/'");
    } finally {
      fs.unlinkSync(actionsYamlPath);
      fs.unlinkSync(notebookPath);
    }
  });

  await runTest("CLI Flow 4D: dataform compile --watch recompiles after a change", () => {
    check(
      !!binary.bundlePath,
      "bundle.js could not be located next to the dataform launcher; pass --dataform-bin <bundle.js>",
    );
    return new Promise<void>((resolve, reject) => {
      // Spawn node directly, without cmd.exe or PowerShell in between, so that child.kill()
      // terminates the dataform process itself rather than the shell.
      const child = spawn(process.execPath, [binary.bundlePath, "compile", projectDir, "--watch"], {
        env: process.env,
      });
      let output = "";
      let changeWritten = false;
      let finished = false;
      const compiledCount = () => output.split("Compiled 1 action(s).").length - 1;
      const finish = (error?: Error) => {
        if (finished) {
          return;
        }
        finished = true;
        clearTimeout(timer);
        child.kill();
        fs.writeFileSync(sampleTablePath, SAMPLE_TABLE_SQLX);
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      };
      const timer = setTimeout(
        () => finish(new Error(`Timed out waiting for a recompilation. Output:\n${output}`)),
        WATCH_TIMEOUT_MILLIS,
      );
      const onData = (chunk: Buffer) => {
        output += chunk.toString();
        if (!changeWritten && output.includes("Watching for changes...")) {
          // The watcher is ready; touch a definition and wait for the second compilation.
          changeWritten = true;
          fs.appendFileSync(sampleTablePath, "\n-- watched change\n");
        } else if (changeWritten && compiledCount() >= 2) {
          finish();
        }
      };
      child.stdout.on("data", onData);
      child.stderr.on("data", onData);
      child.on("error", (error) => finish(error));
      child.on("exit", (code) => {
        if (compiledCount() < 2) {
          finish(new Error(`watch exited early with code ${code}. Output:\n${output}`));
        }
      });
    });
  });

  await runTest("CLI Flow 5: dataform run --dry-run --json", () => {
    // run/test read credentials before doing anything; a dry run never contacts BigQuery.
    fs.writeFileSync(
      path.join(projectDir, ".df-credentials.json"),
      JSON.stringify({ projectId: "test-project" }, null, 2),
    );
    const operationPath = path.join(definitionsDir, "sample_operation.sqlx");
    fs.writeFileSync(operationPath, 'config {\n  type: "operations"\n}\n\nSELECT 1 AS op_col\n');
    try {
      const result = execCli([
        "run",
        projectDir,
        "--dry-run",
        "--json",
        "--actions",
        "sample_operation",
      ]);
      checkEqual(result.status, 0, `run --dry-run must exit with 0: ${describeResult(result)}`);
      const executionGraph = JSON.parse(result.stdout);
      check(Array.isArray(executionGraph.actions), "execution graph must contain actions");
      check(
        executionGraph.actions.some((a: any) => a.target && a.target.name === "sample_operation"),
        "execution graph must include sample_operation",
      );
    } finally {
      fs.unlinkSync(operationPath);
    }
  });

  await runTest("CLI Flow 6: dataform test", () => {
    const noTestsResult = execCli(["test", projectDir]);
    checkEqual(noTestsResult.status, 1, `test must exit with 1: ${describeResult(noTestsResult)}`);
    checkIncludes(noTestsResult.stderr, "No unit tests found.", "test must report missing tests");

    const testPath = path.join(definitionsDir, "table_test.sqlx");
    fs.writeFileSync(
      testPath,
      'config {\n  type: "test",\n  dataset: "sample_table"\n}\n\n' +
        'input "sample_table" {\n  SELECT\n    1 AS sample_col\n}\n\nSELECT\n  1 AS sample_col\n',
    );
    try {
      // Only compilation and test discovery are verified; executing the test needs BigQuery.
      const result = execCli(["test", projectDir], { timeoutMillis: 60_000 });
      checkIncludes(result.stdout, "Running 1 unit tests...", "test must discover the unit test");
    } finally {
      fs.unlinkSync(testPath);
    }
  });

  await runTest("CLI Flow 7: dataform format and --check with CRLF line endings", () => {
    const filePath = path.join(definitionsDir, "to_format.sqlx");
    try {
      fs.writeFileSync(filePath, 'config {    type:   "table"   }   SELECT   1  AS  num');
      const unformattedCheck = execCli(["format", projectDir, "--check"]);
      checkEqual(
        unformattedCheck.status,
        1,
        `--check must fail: ${describeResult(unformattedCheck)}`,
      );
      checkIncludes(unformattedCheck.stderr, "to_format.sqlx", "--check must list the file");

      const format = execCli(["format", projectDir]);
      checkEqual(format.status, 0, `format must exit with 0: ${describeResult(format)}`);
      const formatted = fs.readFileSync(filePath, "utf8");
      check(!formatted.includes("\r\n"), "format must write LF line endings");

      // The same formatted content checked out with CRLF (git core.autocrlf=true) must pass.
      fs.writeFileSync(filePath, formatted.replace(/\n/g, "\r\n"));
      const crlfCheck = execCli(["format", projectDir, "--check"]);
      checkEqual(crlfCheck.status, 0, `--check must accept CRLF: ${describeResult(crlfCheck)}`);
      checkIncludes(crlfCheck.stdout, "All files are formatted correctly", "--check must pass");
    } finally {
      fs.unlinkSync(filePath);
    }
  });

  await runTest("CLI Flow 8: dataform init-creds --help", () => {
    const result = execCli(["init-creds", "--help"]);
    checkEqual(result.status, 0, `init-creds --help must exit with 0: ${describeResult(result)}`);
    checkIncludes(result.stdout, ".df-credentials.json", "help must mention the credentials file");
    checkIncludes(result.stdout, "--test-connection", "help must mention --test-connection");
  });
}

async function main(): Promise<void> {
  console.log("=== Dataform CLI smoke tests ===");
  console.log(`OS: ${os.platform()} ${os.release()}, Node: ${process.version}`);
  console.log(`Shell: ${shellMode}`);
  console.log(`Launcher: ${binary.shimPath || "(none)"}`);
  console.log(`Bundle: ${binary.bundlePath || "(not found)"}`);
  console.log(`Core tarball: ${coreTarball || "(none; installing from the npm registry)"}`);
  console.log("================================\n");

  const projectDir = createTempDir("df-smoke-");
  try {
    await runCliFlows(projectDir);
  } finally {
    removeDir(projectDir);
  }

  console.log(`\nResults: ${passed} passed, ${failed} failed.`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((error) => {
  console.error("Unhandled error in the smoke test runner:", error);
  process.exit(1);
});
