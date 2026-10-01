import { expect } from "chai";
import * as fs from "fs";
import * as path from "path";

import { ModuleResolver } from "df/common/vm/module_resolver";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

const EXTENSIONS = [".js", ".json", ".sqlx"];

function writeFile(filePath: string, contents: string) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, contents);
}

suite("ModuleResolver", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  const newProjectDir = () => fs.realpathSync(tmpDirFixture.createNewTmpDir());

  test("prefers a file over a directory with the same name", () => {
    const projectDir = newProjectDir();
    writeFile(path.join(projectDir, "includes", "helpers.js"), "");
    writeFile(path.join(projectDir, "includes", "helpers", "index.js"), "");

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    const expected = path.join(projectDir, "includes", "helpers.js");
    expect(resolver.resolve("./includes/helpers", path.join(projectDir, "index.js"))).to.equal(
      expected,
    );
    expect(resolver.resolve("includes/helpers", path.join(projectDir, "index.js"))).to.equal(
      expected,
    );
  });

  test("falls back to the directory index when there is no matching file", () => {
    const projectDir = newProjectDir();
    writeFile(path.join(projectDir, "includes", "helpers", "index.sqlx"), "");

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    expect(resolver.resolve("includes/helpers", path.join(projectDir, "index.js"))).to.equal(
      path.join(projectDir, "includes", "helpers", "index.sqlx"),
    );
  });

  test("gives a nested dependency its own copy instead of the top-level one", () => {
    const projectDir = newProjectDir();
    const pkgA = path.join(projectDir, "node_modules", "df-test-a");
    writeFile(path.join(pkgA, "index.js"), "module.exports = require('df-test-b');");
    writeFile(path.join(pkgA, "node_modules", "df-test-b", "index.js"), "");
    writeFile(path.join(projectDir, "node_modules", "df-test-b", "index.js"), "");

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    expect(resolver.resolve("df-test-b", path.join(pkgA, "index.js"))).to.equal(
      path.join(pkgA, "node_modules", "df-test-b", "index.js"),
    );
    expect(resolver.resolve("df-test-b", path.join(projectDir, "index.js"))).to.equal(
      path.join(projectDir, "node_modules", "df-test-b", "index.js"),
    );
  });

  test("honours the package.json exports field", () => {
    const projectDir = newProjectDir();
    const pkgDir = path.join(projectDir, "node_modules", "df-test-exports");
    writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({
        name: "df-test-exports",
        main: "wrong.js",
        exports: { ".": "./lib/main.js" },
      }),
    );
    writeFile(path.join(pkgDir, "wrong.js"), "");
    writeFile(path.join(pkgDir, "lib", "main.js"), "");

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    expect(resolver.resolve("df-test-exports", path.join(projectDir, "index.js"))).to.equal(
      path.join(pkgDir, "lib", "main.js"),
    );
  });

  test("prefers an npm package over a project file with the same name", () => {
    const projectDir = newProjectDir();
    writeFile(path.join(projectDir, "df-test-lib.js"), "");
    writeFile(path.join(projectDir, "node_modules", "df-test-lib", "index.js"), "");

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    expect(resolver.resolve("df-test-lib", path.join(projectDir, "index.js"))).to.equal(
      path.join(projectDir, "node_modules", "df-test-lib", "index.js"),
    );
  });

  test("rejects a package that only exists above the project directory", () => {
    const outerDir = newProjectDir();
    const projectDir = path.join(outerDir, "project");
    fs.mkdirSync(projectDir);
    writeFile(path.join(outerDir, "node_modules", "df-test-hoisted", "index.js"), "");

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    expect(() => resolver.resolve("df-test-hoisted", path.join(projectDir, "index.js"))).to.throw(
      /outside of project directory/,
    );
  });

  test("rejects node_modules symlinks pointing outside the project directory", () => {
    const outsideDir = newProjectDir();
    fs.writeFileSync(path.join(outsideDir, "external.js"), "module.exports = 'escaped';");

    const projectDir = newProjectDir();
    const nodeModulesDir = path.join(projectDir, "node_modules");
    fs.mkdirSync(nodeModulesDir);
    fs.symlinkSync(outsideDir, path.join(nodeModulesDir, "symlinked-pkg"));

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    expect(() =>
      resolver.resolve("symlinked-pkg/external", path.join(projectDir, "index.js")),
    ).to.throw(/outside of project directory/);
  });

  test("attaches the underlying resolution error when a module cannot be found", () => {
    const projectDir = newProjectDir();
    const pkgDir = path.join(projectDir, "node_modules", "df-test-exports");
    writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({ name: "df-test-exports", exports: { ".": "./main.js" } }),
    );
    writeFile(path.join(pkgDir, "main.js"), "");
    writeFile(path.join(pkgDir, "internal.js"), "");

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    let caught: any = null;
    try {
      resolver.resolve("df-test-exports/internal", path.join(projectDir, "index.js"));
    } catch (e) {
      caught = e;
    }
    expect(caught).to.not.equal(null);
    expect(caught.code).to.equal("MODULE_NOT_FOUND");
    expect(caught.message).to.include("Cannot find module 'df-test-exports/internal'");
    expect(caught.cause.code).to.equal("ERR_PACKAGE_PATH_NOT_EXPORTED");
  });

  test("caches resolution results", () => {
    const projectDir = newProjectDir();
    const helperFile = path.join(projectDir, "helper.js");
    fs.writeFileSync(helperFile, "module.exports = { count: 1 };");

    const resolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    const fromPath = path.join(projectDir, "index.js");
    expect(resolver.resolve("./helper", fromPath)).to.equal(helperFile);

    // Without the cache, the second lookup would fail because the file is gone.
    fs.unlinkSync(helperFile);
    expect(resolver.resolve("./helper", fromPath)).to.equal(helperFile);

    // A fresh resolver has no cache, so it fails.
    const freshResolver = new ModuleResolver({ projectDir, extensions: EXTENSIONS });
    expect(() => freshResolver.resolve("./helper", fromPath)).to.throw(/Cannot find module/);
  });
});
