import { expect } from "chai";
import * as fs from "fs";
import * as path from "path";
import { VmRunner } from "df/common/vm/vm_runner";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("VmRunner", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("executes basic script and returns return value", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const runner = new VmRunner({ projectDir: tmpDir });
    const result = runner.run("return 40 + 2;");
    expect(result).to.equal(42);
  });

  test("returns module.exports when no explicit return statement exists", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const runner = new VmRunner({ projectDir: tmpDir });
    const result = runner.run("module.exports = { value: 'hello' };");
    expect(result).to.deep.equal({ value: "hello" });
  });

  test("parses JSON files in run method when filename has .json extension", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const runner = new VmRunner({ projectDir: tmpDir });
    const result = runner.run(
      JSON.stringify({ name: "dataform-test", active: true }),
      path.join(tmpDir, "config.json"),
    );
    expect(result).to.deep.equal({ name: "dataform-test", active: true });
  });

  test("resolves relative requires and json files", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(tmpDir, "config.json"), JSON.stringify({ name: "test-project" }));
    fs.mkdirSync(path.join(tmpDir, "sub"));
    fs.writeFileSync(
      path.join(tmpDir, "sub", "helper.js"),
      "module.exports = { greet: (x) => `Hello ${x}` };",
    );

    const runner = new VmRunner({ projectDir: tmpDir });
    const result = runner.run(`
      const config = require("./config.json");
      const { greet } = require("./sub/helper");
      return greet(config.name);
    `);
    expect(result).to.equal("Hello test-project");
  });

  test("resolves project-relative requires (without leading ./)", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    fs.mkdirSync(path.join(tmpDir, "includes"));
    fs.writeFileSync(
      path.join(tmpDir, "includes", "math.js"),
      "module.exports = { add: (a, b) => a + b };",
    );

    const runner = new VmRunner({ projectDir: tmpDir });
    const result = runner.run(`
      const math = require("includes/math");
      return math.add(10, 20);
    `);
    expect(result).to.equal(30);
  });

  test("applies compiler hook to custom sourceExtensions", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(tmpDir, "model.sqlx"), "SELECT 1 AS id");

    const runner = new VmRunner({
      projectDir: tmpDir,
      sourceExtensions: ["js", "sqlx"],
      compiler: (code, filePath) => {
        if (filePath.endsWith(".sqlx")) {
          return `module.exports = { query: ${JSON.stringify(code.trim())}, file: ${JSON.stringify(filePath)} };`;
        }
        return code;
      },
    });

    const result = runner.run(`
      const model = require("./model.sqlx");
      return model;
    `);
    expect(result.query).to.equal("SELECT 1 AS id");
    expect(result.file).to.equal(path.join(tmpDir, "model.sqlx"));
  });

  test("handles circular require without crashing", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(tmpDir, "a.js"),
      `
        exports.name = "moduleA";
        const b = require("./b");
        exports.getBName = () => b.name;
      `,
    );
    fs.writeFileSync(
      path.join(tmpDir, "b.js"),
      `
        exports.name = "moduleB";
        const a = require("./a");
        exports.getAName = () => a.name;
      `,
    );

    const runner = new VmRunner({ projectDir: tmpDir });
    const result = runner.run(`
      const a = require("./a");
      const b = require("./b");
      return { aToB: a.getBName(), bToA: b.getAName() };
    `);
    expect(result.aToB).to.equal("moduleB");
    expect(result.bToA).to.equal("moduleA");
  });

  test("allows configured builtin modules and rejects unallowed ones", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const runner = new VmRunner({
      projectDir: tmpDir,
      builtinModules: ["path"],
    });

    const pathResult = runner.run(`
      const path = require("path");
      return path.join("foo", "bar");
    `);
    expect(pathResult).to.equal(path.join("foo", "bar"));

    expect(() => {
      runner.run(`require("fs");`);
    }).to.throw(/Access to built-in module 'fs' is not allowed/);
  });

  test("intercepts mockModules", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const mockCore = {
      version: "9.9.9",
      compiler: () => "compiled",
    };

    const runner = new VmRunner({
      projectDir: tmpDir,
      mockModules: {
        "@dataform/core": mockCore,
      },
    });

    const result = runner.run(`
      const core = require("@dataform/core");
      return core.version;
    `);
    expect(result).to.equal("9.9.9");
  });

  test("does not pollute host global", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const runner = new VmRunner({ projectDir: tmpDir });

    runner.run(`global.pollutedState = "in-sandbox";`);
    expect((global as any).pollutedState).to.equal(undefined);
  });

  test("retains context global state across run calls", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const runner = new VmRunner({
      projectDir: tmpDir,
      sandbox: {
        injectedValue: 123,
      },
    });

    const result = runner.run(`
      global.customState = "active";
      return injectedValue + 1;
    `);
    expect(result).to.equal(124);

    const state = runner.run("return global.customState;");
    expect(state).to.equal("active");
  });

  test("removes module from cache when module execution throws and re-throws on next require", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const brokenFile = path.join(tmpDir, "broken.js");
    fs.writeFileSync(brokenFile, "throw new Error('boom');");

    const runner = new VmRunner({ projectDir: tmpDir });

    expect(() => runner.run(`require("./broken");`)).to.throw("boom");
    // Ensure second require also throws and does not return an empty cached exports object
    expect(() => runner.run(`require("./broken");`)).to.throw("boom");
  });

  test("rejects requires that escape projectDir via relative or absolute path traversal", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const outsideDir = tmpDirFixture.createNewTmpDir();
    const secretFile = path.join(outsideDir, "secret.json");
    fs.writeFileSync(secretFile, JSON.stringify({ secret: "sensitive" }));

    const runner = new VmRunner({ projectDir: tmpDir });

    // Relative path traversal
    const relativePath = path.relative(tmpDir, secretFile);
    expect(() => runner.run(`require(${JSON.stringify(relativePath)});`)).to.throw(
      /outside of project directory/,
    );

    // Absolute path traversal
    expect(() => runner.run(`require(${JSON.stringify(secretFile)});`)).to.throw(
      /outside of project directory/,
    );
  });

  test("rejects requires that escape projectDir via customResolve", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const outsideDir = tmpDirFixture.createNewTmpDir();
    const secretFile = path.join(outsideDir, "secret.json");
    fs.writeFileSync(secretFile, JSON.stringify({ secret: "sensitive" }));

    const runner = new VmRunner({
      projectDir: tmpDir,
      resolve: (moduleName) => path.resolve(outsideDir, moduleName),
    });

    expect(() => runner.run(`require("secret.json");`)).to.throw(/outside of project directory/);
  });

  test("resolves relative paths from subfolders relative to caller directory without custom resolve", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const subDir = path.join(tmpDir, "models", "sub");
    fs.mkdirSync(subDir, { recursive: true });

    const rootHelper = path.join(tmpDir, "helper.js");
    fs.writeFileSync(rootHelper, "module.exports = 'root';");

    const subHelper = path.join(subDir, "helper.js");
    fs.writeFileSync(subHelper, "module.exports = 'sub';");

    const subCaller = path.join(subDir, "caller.js");
    fs.writeFileSync(subCaller, "module.exports = require('./helper');");

    const runner = new VmRunner({ projectDir: tmpDir });
    const result = runner.require("./models/sub/caller");
    expect(result).to.equal("sub");
  });

  test("allows requiring external files when explicitly permitted via allowedExternalPaths", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const sharedDir = tmpDirFixture.createNewTmpDir();
    const sharedFile = path.join(sharedDir, "shared.json");
    fs.writeFileSync(sharedFile, JSON.stringify({ shared: "data" }));

    const runner = new VmRunner({
      projectDir: tmpDir,
      allowedExternalPaths: [sharedDir],
    });

    const result = runner.run(`
      const data = require(${JSON.stringify(sharedFile)});
      return data.shared;
    `);
    expect(result).to.equal("data");
  });

  test("supports custom env and envAllowlist, and defaults to empty env", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    process.env.TEST_HOST_SECRET = "secret_123";
    process.env.TEST_PUBLIC_VAR = "public_abc";

    try {
      // Default: empty environment to prevent leaking host secrets
      const defaultRunner = new VmRunner({ projectDir: tmpDir });
      const defaultEnv = defaultRunner.run("return process.env;");
      expect(defaultEnv.TEST_HOST_SECRET).to.equal(undefined);
      expect(defaultEnv.TEST_PUBLIC_VAR).to.equal(undefined);
      expect(Object.keys(defaultEnv)).to.deep.equal([]);

      // With envAllowlist
      const allowlistRunner = new VmRunner({
        projectDir: tmpDir,
        envAllowlist: ["TEST_PUBLIC_VAR"],
      });
      const allowlistEnv = allowlistRunner.run("return process.env;");
      expect(allowlistEnv.TEST_PUBLIC_VAR).to.equal("public_abc");
      expect(allowlistEnv.TEST_HOST_SECRET).to.equal(undefined);

      // With custom env record
      const customRunner = new VmRunner({
        projectDir: tmpDir,
        env: { CUSTOM_KEY: "custom_value" },
      });
      const customEnv = customRunner.run("return process.env;");
      expect(customEnv.CUSTOM_KEY).to.equal("custom_value");
      expect(customEnv.TEST_PUBLIC_VAR).to.equal(undefined);
    } finally {
      delete process.env.TEST_HOST_SECRET;
      delete process.env.TEST_PUBLIC_VAR;
    }
  });

  test("compiles and requires .ipynb and .md files via compiler hook", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(tmpDir, "notebook.ipynb"),
      JSON.stringify({ cells: [{ cell_type: "code", source: ["print('hello')"] }] }),
    );
    fs.writeFileSync(path.join(tmpDir, "doc.md"), "# Hello Documentation");

    const runner = new VmRunner({
      projectDir: tmpDir,
      sourceExtensions: ["js", "json", "ipynb", "md"],
      compiler: (code, filePath) => {
        if (filePath.endsWith(".ipynb")) {
          return `module.exports = { asJson: JSON.parse(${JSON.stringify(code)}) };`;
        }
        if (filePath.endsWith(".md")) {
          return `module.exports = { asMarkdown: ${JSON.stringify(code)} };`;
        }
        return code;
      },
    });

    const result = runner.run(`
      const notebook = require("./notebook.ipynb");
      const doc = require("./doc.md");
      return {
        cellType: notebook.asJson.cells[0].cell_type,
        docTitle: doc.asMarkdown.trim()
      };
    `);
    expect(result.cellType).to.equal("code");
    expect(result.docTitle).to.equal("# Hello Documentation");
  });

  test("preserves V8 CallSite file paths and line numbers in stack traces", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const errorFile = path.join(tmpDir, "faulty.sqlx");
    fs.writeFileSync(errorFile, "throw new Error('boom');");

    const runner = new VmRunner({
      projectDir: tmpDir,
      sourceExtensions: ["sqlx"],
      compiler: (code) => code,
    });

    let caughtError: Error | null = null;
    try {
      runner.run(`require("./faulty.sqlx");`);
    } catch (e) {
      caughtError = e;
    }

    expect(caughtError).to.not.equal(null);
    expect(caughtError!.stack).to.include(errorFile);
  });

  test("caches module resolution results across multiple requires", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const helperFile = path.join(tmpDir, "helper.js");
    fs.writeFileSync(helperFile, "module.exports = { count: 1 };");

    const runner = new VmRunner({ projectDir: tmpDir });
    const resolvedFirst = runner.resolve("./helper", path.join(tmpDir, "index.js"));
    const resolvedSecond = runner.resolve("./helper", path.join(tmpDir, "index.js"));
    expect(resolvedFirst).to.equal(helperFile);
    expect(resolvedSecond).to.equal(helperFile);
  });

  test("shares Uint8Array constructor with host across realm boundary", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    let receivedBytes: any = null;
    const runner = new VmRunner({
      projectDir: tmpDir,
      mockModules: {
        "@dataform/core": {
          jitCompiler: () => ({
            compile: (bytes: Uint8Array) => {
              receivedBytes = bytes;
              return new Uint8Array([bytes[0] + 1, bytes[1] + 1]);
            },
          }),
        },
      },
    });

    const result = runner.run(`
      const { jitCompiler } = require("@dataform/core");
      const compiler = jitCompiler();
      const input = new Uint8Array([10, 20]);
      const output = compiler.compile(input);
      module.exports = { input, output };
    `);

    expect(receivedBytes).to.be.an.instanceOf(Uint8Array);
    expect(receivedBytes[0]).to.equal(10);
    expect(result.input).to.be.an.instanceOf(Uint8Array);
    expect(result.output).to.be.an.instanceOf(Uint8Array);
    expect(Array.from(result.output)).to.deep.equal([11, 21]);
  });

  test("enforces allowedModules restriction when specified", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const runner = new VmRunner({
      projectDir: tmpDir,
      allowedModules: ["@dataform/*"],
      mockModules: {
        "@dataform/core": { name: "core" },
        "other-pkg": { name: "other" },
      },
    });

    // Mocked modules are accessible
    expect(runner.require("@dataform/core")).to.deep.equal({ name: "core" });

    // Non-allowed non-mock module fails
    let err: any = null;
    try {
      runner.require("unallowed-pkg");
    } catch (e) {
      err = e;
    }
    expect(err).to.not.equal(null);
    expect(err.message).to.include("Access to module 'unallowed-pkg' is not allowed");

    // Project-relative internal files are still allowed even when allowedModules is specified
    fs.mkdirSync(path.join(tmpDir, "includes"));
    fs.writeFileSync(path.join(tmpDir, "includes", "helper.js"), "module.exports = { ok: true };");
    expect(runner.require("includes/helper")).to.deep.equal({ ok: true });
  });

  test("does not get stuck in infinite recursion on self-referential package.json main", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const subDir = path.join(tmpDir, "loop_pkg");
    fs.mkdirSync(subDir);
    fs.writeFileSync(path.join(subDir, "package.json"), JSON.stringify({ main: "." }));
    fs.writeFileSync(path.join(subDir, "index.js"), "module.exports = { loaded: true };");

    const runner = new VmRunner({ projectDir: tmpDir });
    const result = runner.require("./loop_pkg", path.join(tmpDir, "index.js"));
    expect(result).to.deep.equal({ loaded: true });
  });

  test("re-throws unexpected errors from customResolve", () => {
    const tmpDir = tmpDirFixture.createNewTmpDir();
    const runner = new VmRunner({
      projectDir: tmpDir,
      resolve: (moduleName) => {
        if (moduleName === "fail-now") {
          throw new TypeError("unexpected resolve error");
        }
        return path.join(tmpDir, `${moduleName}.js`);
      },
    });

    let caught: any = null;
    try {
      runner.resolve("fail-now", path.join(tmpDir, "index.js"));
    } catch (e) {
      caught = e;
    }
    expect(caught).to.not.equal(null);
    expect(caught).to.be.an.instanceOf(TypeError);
    expect(caught.message).to.equal("unexpected resolve error");
  });

  test("enforces isPathContained on node_modules symlinks pointing outside projectDir", () => {
    const outsideDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(outsideDir, "external.js"), "module.exports = 'escaped';");

    const projectDir = tmpDirFixture.createNewTmpDir();
    const nodeModulesDir = path.join(projectDir, "node_modules");
    fs.mkdirSync(nodeModulesDir);
    fs.symlinkSync(outsideDir, path.join(nodeModulesDir, "symlinked-pkg"));

    const runner = new VmRunner({ projectDir });
    let caught: any = null;
    try {
      runner.resolve("symlinked-pkg/external", path.join(projectDir, "index.js"));
    } catch (e) {
      caught = e;
    }
    expect(caught).to.not.equal(null);
    expect(caught.message).to.include("outside of project directory");
  });
});
