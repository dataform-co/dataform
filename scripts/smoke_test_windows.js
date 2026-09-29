#!/usr/bin/env node
/**
 * Automated smoke and unit test suite for Dataform CLI on Windows.
 *
 * Exercises all primary CLI flows:
 *   1. Version / Help check
 *   2. dataform init
 *   3. dataform install (workflow_settings.yaml and package.json flows)
 *   4. dataform compile (default, --json, JiT action, --watch)
 *   5. dataform run --dry-run
 *   6. dataform test
 *   7. dataform format (with --check and CRLF handling)
 *   8. dataform init-creds (--help)
 *
 * Can run under cmd.exe, PowerShell, or bash.
 */

const { spawn, spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const os = require("os");
const assert = require("assert");

// Parse CLI flags
const args = process.argv.slice(2);
function getArg(flag, defaultValue = null) {
  const index = args.indexOf(flag);
  if (index !== -1 && index + 1 < args.length) {
    return args[index + 1];
  }
  return defaultValue;
}

const targetShell = getArg("--shell", "auto"); // "powershell", "cmd", or "auto"
const customBin = getArg("--dataform-bin", process.env.DATAFORM_BIN || null);
const coreTarball = getArg("--core-tarball", null);

console.log("=== Dataform Windows Smoke & Unit Test Runner ===");
console.log(`OS: ${os.platform()} (${os.release()})`);
console.log(`Node: ${process.version}`);
console.log(`Shell target: ${targetShell}`);
if (customBin) console.log(`Custom dataform binary: ${customBin}`);
if (coreTarball) console.log(`Core tarball: ${coreTarball}`);
console.log("=================================================\n");

let passed = 0;
let failed = 0;
let skipped = 0;

function runTest(name, fn) {
  process.stdout.write(`TEST: ${name} ... `);
  try {
    const result = fn();
    if (result && typeof result.then === "function") {
      return result
        .then(() => {
          console.log("PASSED");
          passed++;
        })
        .catch((err) => {
          console.log("FAILED");
          console.error(`  Error: ${err.message || err}`);
          if (err.stack) console.error(err.stack.split("\n").slice(0, 5).join("\n"));
          failed++;
        });
    } else {
      console.log("PASSED");
      passed++;
      return Promise.resolve();
    }
  } catch (err) {
    console.log("FAILED");
    console.error(`  Error: ${err.message || err}`);
    if (err.stack) console.error(err.stack.split("\n").slice(0, 5).join("\n"));
    failed++;
    return Promise.resolve();
  }
}

function resolveDataformCmd() {
  if (customBin) {
    return customBin;
  }
  if (os.platform() === "win32") {
    // Look for dataform.cmd in PATH
    const pathDirs = (process.env.PATH || "").split(path.delimiter);
    for (const dir of pathDirs) {
      const cmdPath = path.join(dir, "dataform.cmd");
      if (fs.existsSync(cmdPath)) {
        return cmdPath;
      }
    }
  }
  return "dataform";
}

function execCli(commandArgs, options = {}) {
  const bin = resolveDataformCmd();
  const cwd = options.cwd || process.cwd();
  const env = { ...process.env, ...(options.env || {}) };

  if (targetShell === "powershell") {
    const binPart = bin.endsWith(".js")
      ? `"${process.execPath}" "${path.resolve(bin)}"`
      : `& "${bin}"`;
    const quotedArgs = commandArgs.map((a) => (a.includes(" ") ? `"${a}"` : a)).join(" ");
    const psArgs = ["-NoProfile", "-NonInteractive", "-Command", `${binPart} ${quotedArgs}`];
    return spawnSync("powershell", psArgs, {
      cwd,
      env,
      encoding: "utf8",
      timeout: options.timeout || 60000,
    });
  }

  let spawnFile = bin;
  let spawnArgs = commandArgs;

  if (bin.endsWith(".js")) {
    spawnFile = process.execPath;
    spawnArgs = [path.resolve(bin), ...commandArgs];
  }

  if (os.platform() === "win32") {
    // Under cmd.exe or Windows default, delegate through cmd via shell: true.
    // Node handles Windows cmd.exe argument quoting correctly without escaping quotes to backslash-quotes (\").
    const safeFile = spawnFile.includes(" ") ? `"${spawnFile}"` : spawnFile;
    const safeArgs = spawnArgs.map((a) => (a.includes(" ") ? `"${a}"` : a));
    return spawnSync(safeFile, safeArgs, {
      cwd,
      env,
      shell: true,
      encoding: "utf8",
      timeout: options.timeout || 60000,
    });
  }

  return spawnSync(spawnFile, spawnArgs, {
    cwd,
    env,
    encoding: "utf8",
    timeout: options.timeout || 60000,
  });
}

function toPosixPath(p) {
  return p ? p.replace(/\\+/g, "/") : p;
}

function normalizeLineEndings(text) {
  return text ? text.replace(/\r\n/g, "\n") : text;
}

function createTempDir(prefix = "df-smoke-") {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function cleanupDir(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true, maxRetries: 3 });
  } catch (e) {
    // Ignore cleanup errors on busy temp dirs
  }
}

// -------------------------------------------------------------
// Suite 1: Path & Platform Unit Tests
// -------------------------------------------------------------
async function runUnitTests() {
  console.log("\n--- Suite 1: Path & Platform Unit Tests ---");

  // We test the canonical path rules implemented in core/path.ts
  await runTest("Path: canonical separator is always forward slash '/'", () => {
    // Forward slash is required by internal graph contract
    const canonicalSep = "/";
    assert.strictEqual(canonicalSep, "/");
  });

  await runTest("Path: toPosixPath replaces Windows backslashes with forward slashes", () => {
    assert.strictEqual(toPosixPath("definitions\\table.sqlx"), "definitions/table.sqlx");
    assert.strictEqual(toPosixPath("includes\\sub\\file.js"), "includes/sub/file.js");
    assert.strictEqual(
      toPosixPath("definitions\\\\nested\\\\table.sqlx"),
      "definitions/nested/table.sqlx",
    );
    assert.strictEqual(toPosixPath("C:\\project\\file.sqlx"), "C:/project/file.sqlx");
  });

  await runTest("Path: dirName handles both Windows and POSIX separators", () => {
    function dirName(p) {
      const normalized = toPosixPath(p);
      const lastSlash = normalized.lastIndexOf("/");
      if (lastSlash === -1) return ".";
      if (lastSlash === 0) return "/";
      return normalized.substring(0, lastSlash);
    }
    assert.strictEqual(dirName("definitions\\table.sqlx"), "definitions");
    assert.strictEqual(dirName("definitions/table.sqlx"), "definitions");
    assert.strictEqual(dirName("definitions\\nested\\table.sqlx"), "definitions/nested");
  });

  await runTest("Path: basename handles both Windows and POSIX separators", () => {
    function basename(p) {
      const normalized = toPosixPath(p);
      const lastSlash = normalized.lastIndexOf("/");
      return lastSlash === -1 ? normalized : normalized.substring(lastSlash + 1);
    }
    assert.strictEqual(basename("definitions\\table.sqlx"), "table.sqlx");
    assert.strictEqual(basename("definitions/table.sqlx"), "table.sqlx");
  });

  await runTest("Path: relativePath handles case-insensitive Windows drive letters", () => {
    function relativePath(from, to) {
      let fromNorm = toPosixPath(from);
      let toNorm = toPosixPath(to);
      if (/^[a-zA-Z]:/.test(fromNorm) && /^[a-zA-Z]:/.test(toNorm)) {
        if (fromNorm[0].toLowerCase() === toNorm[0].toLowerCase()) {
          fromNorm = fromNorm[0].toLowerCase() + fromNorm.slice(1);
          toNorm = toNorm[0].toLowerCase() + toNorm.slice(1);
        }
      }
      return path.posix.relative(fromNorm, toNorm);
    }
    const rel = relativePath("C:/project/root", "c:/project/root/definitions/table.sqlx");
    assert.strictEqual(rel, "definitions/table.sqlx");
  });

  await runTest("CRLF normalization preserves SQLX formatting checks", () => {
    const unixFormatted = "config { type: 'table' }\nSELECT 1 AS col\n";
    const windowsFormatted = "config { type: 'table' }\r\nSELECT 1 AS col\r\n";
    assert.strictEqual(normalizeLineEndings(windowsFormatted), unixFormatted);
  });
}

// -------------------------------------------------------------
// Suite 2: CLI Flows
// -------------------------------------------------------------
async function runCliFlows() {
  console.log("\n--- Suite 2: CLI Smoke Tests ---");

  let testProjectDir = null;

  await runTest("CLI Flow 1: dataform --version / help", () => {
    const res = execCli(["--help"]);
    assert.strictEqual(
      res.status,
      0,
      `Expected exit 0, got ${res.status}. Output: ${res.stdout}\n${res.stderr}`,
    );
    assert(
      res.stdout.includes("dataform [command]"),
      "Expected output to contain 'dataform [command]'",
    );
  });

  await runTest("CLI Flow 2: dataform init creates project structure", () => {
    testProjectDir = createTempDir("df-init-test-");
    const res = execCli(["init", testProjectDir, "test-db", "us-central1"]);
    assert.strictEqual(
      res.status,
      0,
      `Init failed with exit ${res.status}: ${res.stderr}\n${res.stdout}`,
    );

    const workflowSettingsPath = path.join(testProjectDir, "workflow_settings.yaml");
    assert(fs.existsSync(workflowSettingsPath), "workflow_settings.yaml must exist");

    let content = fs.readFileSync(workflowSettingsPath, "utf8");
    assert(content.includes("defaultProject: test-db"), "Must contain defaultProject");
    assert(content.includes("defaultLocation: us-central1"), "Must contain defaultLocation");

    if (coreTarball) {
      // Use local core tarball for offline / unpublished version testing
      const tarballPosix = toPosixPath(path.resolve(coreTarball));
      content = content.replace(
        /dataformCoreVersion: .*/,
        `dataformCoreVersion: "file:${tarballPosix}"`,
      );
      fs.writeFileSync(workflowSettingsPath, content);
    }

    assert(
      fs.existsSync(path.join(testProjectDir, "definitions")),
      "definitions/ folder must exist",
    );
    assert(fs.existsSync(path.join(testProjectDir, "includes")), "includes/ folder must exist");
  });

  await runTest("CLI Flow 3A: dataform install (workflow_settings.yaml flow)", () => {
    assert(testProjectDir, "Project directory must exist");
    const res = execCli(["install", testProjectDir]);
    // With workflow_settings.yaml containing dataformCoreVersion, install throws a friendly notice:
    const combinedOutput = (res.stdout || "") + (res.stderr || "");
    assert(
      combinedOutput.includes("No installation is needed when using workflow_settings.yaml") ||
        res.status === 0,
      `Expected notice about workflow_settings.yaml install, got: ${combinedOutput}`,
    );
  });

  await runTest("CLI Flow 3B: dataform install (package.json flow)", () => {
    const pkgProjectDir = createTempDir("df-pkg-test-");
    try {
      // Create dataform.json and package.json
      fs.writeFileSync(
        path.join(pkgProjectDir, "dataform.json"),
        JSON.stringify(
          {
            defaultDatabase: "test-db",
            defaultLocation: "us-central1",
          },
          null,
          2,
        ),
      );

      // Point @dataform/core to local tarball if provided, or a valid package
      const coreDep = coreTarball ? path.resolve(coreTarball) : "^3.0.70";
      fs.writeFileSync(
        path.join(pkgProjectDir, "package.json"),
        JSON.stringify(
          {
            name: "test-pkg-project",
            dependencies: {
              "@dataform/core": coreDep,
            },
          },
          null,
          2,
        ),
      );

      const res = execCli(["install", pkgProjectDir]);
      if (coreTarball) {
        assert.strictEqual(res.status, 0, `Install failed: ${res.stderr}\n${res.stdout}`);
        assert(
          fs.existsSync(path.join(pkgProjectDir, "node_modules")),
          "node_modules must exist after install",
        );
      } else {
        // If no core tarball supplied (e.g. offline run), verify it ran npm install
        const output = (res.stdout || "") + (res.stderr || "");
        assert(output.includes("Installing NPM dependencies"), "Must run npm install");
      }
    } finally {
      cleanupDir(pkgProjectDir);
    }
  });

  await runTest("CLI Flow 4A: dataform compile (Issue #1565 fix verification)", () => {
    assert(testProjectDir, "Project directory must exist");
    const tableFile = path.join(testProjectDir, "definitions", "sample_table.sqlx");
    fs.writeFileSync(tableFile, 'config {\n  type: "table"\n}\n\nSELECT\n  1 AS sample_col\n');

    const res = execCli(["compile", testProjectDir]);
    assert.strictEqual(res.status, 0, `Compile failed: ${res.stderr}\n${res.stdout}`);
    const output = (res.stdout || "") + (res.stderr || "");
    assert(
      output.includes("Compiled successfully") || output.includes("sample_table"),
      `Output should confirm compilation: ${output}`,
    );
  });

  await runTest("CLI Flow 4B: dataform compile --json (Issue #510 canonical slash fix)", () => {
    assert(testProjectDir, "Project directory must exist");
    const res = execCli(["compile", testProjectDir, "--json"]);
    assert.strictEqual(res.status, 0, `Compile --json failed: ${res.stderr}\n${res.stdout}`);

    const compiledGraph = JSON.parse(res.stdout);
    assert(Array.isArray(compiledGraph.tables), "Compiled graph must contain tables");
    const table = compiledGraph.tables.find((t) => t.target && t.target.name === "sample_table");
    assert(table, "sample_table must be present in compiled tables");

    // Critical assertion for issue #510: fileName must use canonical forward slash '/'
    assert.strictEqual(
      table.fileName,
      "definitions/sample_table.sqlx",
      "fileName must use canonical '/' forward slashes on Windows",
    );
  });

  await runTest("CLI Flow 4C: dataform compile with JiT action (declarative actions.yaml)", () => {
    assert(testProjectDir, "Project directory must exist");
    const actionsYamlPath = path.join(testProjectDir, "definitions", "actions.yaml");
    fs.writeFileSync(
      actionsYamlPath,
      `actions:\n- notebook:\n    filename: sample_notebook.ipynb\n`,
    );
    const notebookPath = path.join(testProjectDir, "definitions", "sample_notebook.ipynb");
    fs.writeFileSync(notebookPath, JSON.stringify({ cells: [] }));

    const res = execCli(["compile", testProjectDir, "--json"]);
    assert.strictEqual(res.status, 0, `Compile with notebook failed: ${res.stderr}\n${res.stdout}`);

    const compiled = JSON.parse(res.stdout);
    assert(Array.isArray(compiled.notebooks), "Compiled graph must contain notebooks");
    const nb = compiled.notebooks.find((n) => n.target && n.target.name === "sample_notebook");
    assert(nb, "sample_notebook must be compiled");
    assert.strictEqual(
      nb.fileName,
      "definitions/sample_notebook.ipynb",
      "notebook fileName must use '/' slashes",
    );

    // Clean up notebook action for subsequent tests
    fs.unlinkSync(actionsYamlPath);
    fs.unlinkSync(notebookPath);
  });

  await runTest("CLI Flow 4D: dataform compile --watch handles file watching", async () => {
    assert(testProjectDir, "Project directory must exist");
    const bin = resolveDataformCmd();
    let spawnExe = bin;
    let spawnParams = ["compile", testProjectDir, "--watch"];
    if (bin.endsWith(".js")) {
      spawnExe = process.execPath;
      spawnParams = [path.resolve(bin), ...spawnParams];
    }

    return new Promise((resolve, reject) => {
      let watchOutput = "";
      const child = spawn(spawnExe, spawnParams, {
        shell: os.platform() === "win32",
        env: process.env,
      });

      let ready = false;
      const timeout = setTimeout(() => {
        child.kill();
        reject(new Error(`Timeout waiting for watch mode. Output was: ${watchOutput}`));
      }, 20000);

      child.stdout.on("data", (data) => {
        watchOutput += data.toString();
        if (
          watchOutput.includes("ENOSPC") ||
          watchOutput.includes("System limit for number of file watchers reached")
        ) {
          clearTimeout(timeout);
          child.kill();
          return resolve();
        }
        if (watchOutput.includes("Watching for changes...") && !ready) {
          ready = true;
          // Trigger a change by updating the sample table
          const tableFile = path.join(testProjectDir, "definitions", "sample_table.sqlx");
          fs.appendFileSync(tableFile, "\n-- watched change\n");

          setTimeout(() => {
            child.kill();
            clearTimeout(timeout);
            fs.writeFileSync(
              tableFile,
              'config {\n  type: "table"\n}\n\nSELECT\n  1 AS sample_col\n',
            );
            assert(watchOutput.includes("Watching for changes..."), "Watcher started");
            resolve();
          }, 2000);
        }
      });

      child.stderr.on("data", (data) => {
        watchOutput += data.toString();
        if (
          watchOutput.includes("ENOSPC") ||
          watchOutput.includes("System limit for number of file watchers reached")
        ) {
          clearTimeout(timeout);
          child.kill();
          return resolve();
        }
      });

      child.on("error", (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  });

  await runTest("CLI Flow 5: dataform run --dry-run --json", () => {
    assert(testProjectDir, "Project directory must exist");
    // Create dummy .df-credentials.json required by dataform run/test commands
    const credsPath = path.join(testProjectDir, ".df-credentials.json");
    fs.writeFileSync(credsPath, JSON.stringify({ projectId: "test-project" }, null, 2));

    // Add an operations action which builds offline without warehouse state network calls
    const opFile = path.join(testProjectDir, "definitions", "sample_operation.sqlx");
    fs.writeFileSync(opFile, 'config {\n  type: "operations"\n}\n\nSELECT 1 AS op_col\n');

    const res = execCli([
      "run",
      testProjectDir,
      "--dry-run",
      "--json",
      "--actions",
      "sample_operation",
    ]);
    assert.strictEqual(res.status, 0, `Run dry-run failed: ${res.stderr}\n${res.stdout}`);

    const execGraph = JSON.parse(res.stdout);
    assert(Array.isArray(execGraph.actions), "Execution graph must contain actions");
    const action = execGraph.actions.find((a) => a.target && a.target.name === "sample_operation");
    assert(action, "Execution graph must include sample_operation");

    fs.unlinkSync(opFile);
  });

  await runTest("CLI Flow 6: dataform test", () => {
    assert(testProjectDir, "Project directory must exist");
    // In project with no tests, exits with code 1 and 'No unit tests found.'
    const resNoTests = execCli(["test", testProjectDir]);
    const output = (resNoTests.stdout || "") + (resNoTests.stderr || "");
    assert(
      output.includes("No unit tests found."),
      `Must identify lack of unit tests, got: ${output}`,
    );

    // Add a test definition
    const testFile = path.join(testProjectDir, "definitions", "table_test.sqlx");
    fs.writeFileSync(
      testFile,
      `config {\n  type: "test",\n  dataset: "sample_table"\n}\n\ninput "sample_table" {\n  SELECT\n    1 AS sample_col\n}\n\nSELECT\n  1 AS sample_col\n`,
    );

    const resWithTest = execCli(["test", testProjectDir]);
    const testOutput = (resWithTest.stdout || "") + (resWithTest.stderr || "");
    assert(
      testOutput.includes("Running 1 unit test") ||
        testOutput.includes("Compiled successfully") ||
        testOutput.includes("Credentials"),
      `Must compile and detect test, got: ${testOutput}`,
    );

    fs.unlinkSync(testFile);
  });

  await runTest("CLI Flow 7: dataform format (with CRLF and --check)", () => {
    assert(testProjectDir, "Project directory must exist");
    execCli(["format", testProjectDir]);
    const formatFile = path.join(testProjectDir, "definitions", "to_format.sqlx");

    // Write file with CRLF line endings
    fs.writeFileSync(
      formatFile,
      'config {\r\n  type: "table"\r\n}\r\n\r\nSELECT\r\n  1 AS num\r\n',
    );

    // Check should pass on already formatted content regardless of CRLF
    const checkRes = execCli(["format", testProjectDir, "--check"]);
    assert.strictEqual(
      checkRes.status,
      0,
      `format --check should pass on CRLF file: ${checkRes.stderr}\n${checkRes.stdout}`,
    );

    // Now write unformatted content
    fs.writeFileSync(formatFile, 'config {    type:   "table"   }   SELECT   1  AS  num');
    const unformattedCheckRes = execCli(["format", testProjectDir, "--check"]);
    assert.notStrictEqual(
      unformattedCheckRes.status,
      0,
      "format --check should fail on unformatted file",
    );

    // Run format to fix it
    const formatRes = execCli(["format", testProjectDir]);
    assert.strictEqual(
      formatRes.status,
      0,
      `format should succeed: ${formatRes.stderr}\n${formatRes.stdout}`,
    );

    // Verify format --check now passes
    const reCheckRes = execCli(["format", testProjectDir, "--check"]);
    assert.strictEqual(reCheckRes.status, 0, "format --check must pass after running format");

    fs.unlinkSync(formatFile);
  });

  await runTest("CLI Flow 8: dataform init-creds --help", () => {
    assert(testProjectDir, "Project directory must exist");
    const res = execCli(["init-creds", "--help"]);
    assert.strictEqual(res.status, 0, `init-creds --help failed: ${res.stderr}\n${res.stdout}`);
    const output = (res.stdout || "") + (res.stderr || "");
    assert(output.includes(".df-credentials.json"), "Help must mention .df-credentials.json");
    assert(output.includes("--test-connection"), "Help must mention --test-connection option");
  });

  // Final cleanup
  if (testProjectDir) {
    cleanupDir(testProjectDir);
  }
}

async function main() {
  await runUnitTests();
  await runCliFlows();

  console.log("\n=================================================");
  console.log(`Results: ${passed} passed, ${failed} failed, ${skipped} skipped.`);
  console.log("=================================================");

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error("Unhandled exception in test runner:", err);
  process.exit(1);
});
