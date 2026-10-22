import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { checkCoreCompatibility, compile, MIN_SUPPORTED_CORE_VERSION } from "df/cli/vm/compile";
import { findInstalledCoreBundle } from "df/cli/vm/core_installation";
import { handleJitRequest, IJitCompileMessage } from "df/cli/vm/jit_worker";
import { decode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";
import { suite, test, writeCorePackage, writeWorkflowSettingsFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

function coreBundlePath(projectDir: string) {
  return path.join(projectDir, "node_modules", "@dataform", "core", "bundle.js");
}

suite("cli/vm", ({ afterEach, before, after }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  // Let host-side require("@dataform/core") resolve to the prebuilt core bundle in the test
  // environment, so that the "does not fall back to the CLI's bundled core" test would catch a
  // regression. Scoped to this suite via before/after.
  const Module = require("module");
  const prebuiltCoreBundle = path.join(
    process.cwd(),
    "core",
    "node_modules",
    "@dataform",
    "core",
    "bundle.js",
  );
  let originalResolveFilename: (...args: any[]) => string;

  before("resolve @dataform/core to the prebuilt bundle", () => {
    originalResolveFilename = Module._resolveFilename;
    Module._resolveFilename = function (request: string) {
      if (request === "@dataform/core") {
        return prebuiltCoreBundle;
      }
      return originalResolveFilename.apply(this, arguments);
    };
  });

  after("restore Module._resolveFilename", () => {
    Module._resolveFilename = originalResolveFilename;
  });

  test("compile() runs end-to-end against prebuilt @dataform/core bundle", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();

    // Copy built @dataform/core from Bazel runfiles into the project's node_modules.
    fs.copySync(
      path.join(process.cwd(), "core", "node_modules"),
      path.join(projectDir, "node_modules"),
    );

    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: test-project
defaultDataset: test-dataset
defaultLocation: US
`,
    );

    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions", "example.sqlx"),
      `
config {
  type: "table",
  name: "example"
}
SELECT 1 AS col
`,
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "actions.yaml"),
      `
actions:
  - notebook:
      filename: test_notebook.ipynb
`,
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "test_notebook.ipynb"),
      JSON.stringify({ cells: [] }),
    );

    const encodedResponse = compile({ projectDir });
    const response = decode64(dataform.CoreExecutionResponse, encodedResponse);

    expect(response.compile).to.be.an("object");
    expect(response.compile.compiledGraph).to.be.an("object");
    const tables = response.compile.compiledGraph.tables;
    expect(tables).to.have.lengthOf(1);
    expect(tables[0].target.name).to.equal("example");
    expect(tables[0].target.schema).to.equal("test-dataset");
    expect(tables[0].target.database).to.equal("test-project");

    const notebooks = response.compile.compiledGraph.notebooks;
    expect(notebooks).to.have.lengthOf(1);
    expect(notebooks[0].target.name).to.equal("test_notebook");
  });

  test("compile() does not expose host environment variables to project code", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.copySync(
      path.join(process.cwd(), "core", "node_modules"),
      path.join(projectDir, "node_modules"),
    );
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: test-project
defaultDataset: test-dataset
defaultLocation: US
`,
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions", "env_probe.js"),
      `publish("env_probe", { type: "table" }).query(
        "SELECT '" + (process.env.DF_TEST_HOST_SECRET || "") + "' AS host_secret, " +
          Object.keys(process.env).length + " AS host_env_keys"
      );`,
    );

    process.env.DF_TEST_HOST_SECRET = "leaked";
    let encodedResponse: string;
    try {
      encodedResponse = compile({ projectDir });
    } finally {
      delete process.env.DF_TEST_HOST_SECRET;
    }

    const response = decode64(dataform.CoreExecutionResponse, encodedResponse);
    const tables = response.compile.compiledGraph.tables;
    expect(tables).to.have.lengthOf(1);
    expect(tables[0].query.trim()).to.equal("SELECT '' AS host_secret, 0 AS host_env_keys");
  });

  test("compile() runs project files in strict mode", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.copySync(
      path.join(process.cwd(), "core", "node_modules"),
      path.join(projectDir, "node_modules"),
    );
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      `
defaultProject: test-project
defaultDataset: test-dataset
defaultLocation: US
`,
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions", "a_leaky.js"),
      `leakedName = "leaked";
publish("a_leaky", { type: "table" }).query("SELECT 1");`,
    );
    fs.writeFileSync(
      path.join(projectDir, "definitions", "b_reader.js"),
      `publish("b_reader", { type: "table" }).query("SELECT '" + typeof leakedName + "' AS t");`,
    );

    const response = decode64(dataform.CoreExecutionResponse, compile({ projectDir }));
    const graph = response.compile.compiledGraph;

    expect(graph.graphErrors.compilationErrors).to.have.lengthOf(1);
    expect(graph.graphErrors.compilationErrors[0].fileName).to.equal("definitions/a_leaky.js");
    expect(graph.graphErrors.compilationErrors[0].message).to.equal("leakedName is not defined");
    expect(graph.tables).to.have.lengthOf(1);
    expect(graph.tables[0].target.name).to.equal("b_reader");
    expect(graph.tables[0].query.trim()).to.equal("SELECT 'undefined' AS t");
  });

  function operationJitRequest(sql: string) {
    return dataform.JitCompilationRequest.create({
      jitCode: `async (ctx) => "${sql}"`,
      target: {
        database: "db",
        schema: "schema",
        name: "test_op",
      },
      compilationTargetType:
        dataform.JitCompilationTargetType.JIT_COMPILATION_TARGET_TYPE_OPERATION,
    });
  }

  async function runJitRequest(message: IJitCompileMessage): Promise<any[]> {
    const messages: any[] = [];
    const origSend = process.send;
    (process as any).send = (msg: any) => messages.push(msg);
    try {
      await handleJitRequest(message);
    } finally {
      (process as any).send = origSend;
    }
    return messages;
  }

  function createProjectWithPrebuiltCore(): { projectDir: string; coreVersion: string } {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.copySync(
      path.join(process.cwd(), "core", "node_modules"),
      path.join(projectDir, "node_modules"),
    );
    const coreVersion = fs.readJsonSync(
      path.join(projectDir, "node_modules", "@dataform", "core", "package.json"),
    ).version;
    return { projectDir, coreVersion };
  }

  test("handleJitRequest compiles request with the project's core", async () => {
    const { projectDir } = createProjectWithPrebuiltCore();

    const messages = await runJitRequest({ request: operationJitRequest("SELECT 1"), projectDir });

    expect(messages).to.have.lengthOf(1);
    expect(messages[0].type).to.equal("jit_response");
    expect(messages[0].response.operation.queries).to.deep.equal(["SELECT 1"]);
  });

  test("handleJitRequest succeeds when the project's core matches the expected version", async () => {
    const { projectDir, coreVersion } = createProjectWithPrebuiltCore();

    const messages = await runJitRequest({
      request: operationJitRequest("SELECT 1"),
      projectDir,
      expectedCoreVersion: coreVersion,
    });

    expect(messages).to.have.lengthOf(1);
    expect(messages[0].type).to.equal("jit_response");
    expect(messages[0].response.operation.queries).to.deep.equal(["SELECT 1"]);
  });

  test("handleJitRequest fails when the project's core differs from the expected version", async () => {
    const { projectDir, coreVersion } = createProjectWithPrebuiltCore();

    const messages = await runJitRequest({
      request: operationJitRequest("SELECT 1"),
      projectDir,
      expectedCoreVersion: "3.0.1",
    });

    expect(messages).to.have.lengthOf(1);
    expect(messages[0].type).to.equal("jit_error");
    expect(messages[0].error).to.include(
      `JiT compilation would use @dataform/core ${coreVersion} from '${coreBundlePath(projectDir)}', ` +
        `but the ` +
        `project was compiled with @dataform/core 3.0.1.`,
    );
  });

  test("handleJitRequest reports a core without a version as unknown", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeCorePackage(projectDir, "module.exports = { jitCompiler: () => ({}) };");

    const messages = await runJitRequest({
      request: operationJitRequest("SELECT 1"),
      projectDir,
      expectedCoreVersion: "3.0.71",
    });

    expect(messages).to.have.lengthOf(1);
    expect(messages[0].type).to.equal("jit_error");
    expect(messages[0].error).to.include(
      `JiT compilation would use @dataform/core of an unknown version from ` +
        `'${coreBundlePath(projectDir)}', but the project was compiled with @dataform/core 3.0.71.`,
    );
  });

  test("handleJitRequest does not fall back to the CLI's bundled core", async () => {
    // Host-side require("@dataform/core") resolves to the prebuilt bundle in this suite (see the
    // `before` hook), so a fallback to the CLI's core would succeed and return a jit_response.
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeCorePackage(projectDir);

    const messages = await runJitRequest({ request: operationJitRequest("SELECT 42"), projectDir });

    expect(messages).to.have.lengthOf(1);
    expect(messages[0].type).to.equal("jit_error");
    expect(messages[0].error).to.include("Could not find @dataform/core");
  });

  test("handleJitRequest fails when the project's core does not support JiT", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeCorePackage(projectDir, `module.exports = { version: "3.0.0" };`);

    const messages = await runJitRequest({
      request: operationJitRequest("SELECT 1"),
      projectDir,
      expectedCoreVersion: "3.0.0",
    });

    expect(messages).to.have.lengthOf(1);
    expect(messages[0].type).to.equal("jit_error");
    expect(messages[0].error).to.include("@dataform/core 3.0.0 does not support JiT compilation");
  });
});

suite("cli/vm findInstalledCoreBundle", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("returns the installed bundle", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeCorePackage(projectDir, "module.exports = {};");

    expect(findInstalledCoreBundle(projectDir)).to.equal(coreBundlePath(projectDir));
  });

  test("explains a missing project directory", () => {
    const projectDir = path.join(tmpDirFixture.createNewTmpDir(), "removed");

    expect(() => findInstalledCoreBundle(projectDir)).to.throw(
      `Could not find @dataform/core: the project directory '${projectDir}' does not exist.`,
    );
  });

  test("tells package.json projects to run dataform install", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeJsonSync(path.join(projectDir, "package.json"), { dependencies: {} });

    expect(() => findInstalledCoreBundle(projectDir)).to.throw(
      `Could not find @dataform/core: '${coreBundlePath(projectDir)}' does not exist. Run ` +
        "`dataform install` in the project directory to install the version from package.json.",
    );
  });

  test("explains that dataformCoreVersion projects run in a temporary directory", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, { dataformCoreVersion: "3.0.71" });

    expect(() => findInstalledCoreBundle(projectDir)).to.throw(
      `Could not find @dataform/core: '${coreBundlePath(projectDir)}' does not exist. Projects ` +
        "that set `dataformCoreVersion` in workflow_settings.yaml are compiled in a temporary " +
        "directory",
    );
  });
});

suite("cli/vm checkCoreCompatibility", () => {
  // Each entry: [cliVersion, coreVersion]
  const supported: Array<[string, string]> = [
    ["3.1.0", MIN_SUPPORTED_CORE_VERSION],
    // Older than 3.0.57, so it uses the caller-file shim.
    ["3.1.0", "3.0.50"],
    ["3.1.0", "3.0.71"],
    ["3.1.0", "3.1.0"],
    // Core can be newer than the CLI if the major version is the same.
    ["3.1.0", "3.2.0"],
    ["3.1.0", "3.1.1-beta.0"],
    // Later CLI versions with major 3 still accept 3.0.x.
    ["3.9.4", "3.0.0"],
    // Unknown or invalid CLI version (e.g. running from source): the check is skipped.
    ["0.0.0", "2.9.0"],
    ["not-a-version", "2.9.0"],
  ];
  supported.forEach(([cliVersion, coreVersion]) => {
    test(`CLI ${cliVersion} accepts @dataform/core ${coreVersion}`, () => {
      expect(() => checkCoreCompatibility(coreVersion, cliVersion)).to.not.throw();
    });
  });

  // Each entry: [cliVersion, coreVersion, version range shown in the error]
  const unsupported: Array<[string, string, string]> = [
    ["3.1.0", "2.9.0", ">=3.0.0 <4.0.0"],
    ["3.1.0", "4.0.0", ">=3.0.0 <4.0.0"],
    ["3.1.0", "4.0.0-alpha.0", ">=3.0.0 <4.0.0"],
    // Pre-release of 3.0.0, which counts as older than 3.0.0.
    ["3.1.0", "3.0.0-alpha.0", ">=3.0.0 <4.0.0"],
    // A CLI with a new major version does not accept Core from the old major.
    ["4.0.0", "3.0.71", ">=4.0.0 <5.0.0"],
  ];
  unsupported.forEach(([cliVersion, coreVersion, range]) => {
    test(`CLI ${cliVersion} rejects @dataform/core ${coreVersion}`, () => {
      expect(() => checkCoreCompatibility(coreVersion, cliVersion)).to.throw(
        `@dataform/core ${coreVersion} is not compatible with @dataform/cli ${cliVersion}. ` +
          `The CLI requires @dataform/core ${range}.`,
      );
    });
  });
});
