import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import {
  buildProjectCopyFilter,
  findProjectIgnoreFiles,
} from "df/cli/api/commands/compile_copy_filter";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("buildProjectCopyFilter", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("with no .gitignore, only .git and node_modules are excluded", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions", "foo.sqlx"), "SELECT 1");
    fs.ensureDirSync(path.join(projectDir, ".venv"));
    fs.ensureDirSync(path.join(projectDir, ".git"));
    fs.ensureDirSync(path.join(projectDir, "node_modules"));
    fs.ensureDirSync(path.join(projectDir, "definitions", "nested", "node_modules"));
    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(projectDir)).to.equal(true);
    expect(filter(path.join(projectDir, "definitions"))).to.equal(true);
    expect(filter(path.join(projectDir, "definitions", "foo.sqlx"))).to.equal(true);
    expect(filter(path.join(projectDir, ".venv"))).to.equal(true);

    expect(filter(path.join(projectDir, ".git"))).to.equal(false);
    expect(filter(path.join(projectDir, "node_modules"))).to.equal(false);
    // Excluded at any depth, not just at the project root.
    expect(filter(path.join(projectDir, "definitions", "nested", "node_modules"))).to.equal(false);
  });

  test("does not dereference symlinks while filtering", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const danglingSymlink = path.join(projectDir, "dangling-link");
    fs.symlinkSync(path.join(projectDir, "missing-target"), danglingSymlink);

    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(danglingSymlink)).to.equal(true);
  });

  test("applies ignore rules to in-project paths beginning with two dots", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const ignoredDir = path.join(projectDir, "..cache");
    fs.ensureDirSync(ignoredDir);
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "..cache/\n");

    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(ignoredDir)).to.equal(false);
  });

  test("the always-ignored floor cannot be overridden by a negation pattern", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, "node_modules"));
    // A project .gitignore is user-controlled and could (unusually, but validly)
    // contain a negation pattern for something we always want to exclude.
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "!node_modules\n");

    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(path.join(projectDir, "node_modules"))).to.equal(false);
  });

  test("filters an actual project copy", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions", "foo.sqlx"), "SELECT 1");
    fs.ensureDirSync(path.join(projectDir, ".venv"));
    fs.writeFileSync(path.join(projectDir, ".venv", "ignored"), "junk");
    fs.ensureDirSync(path.join(projectDir, "node_modules"));
    fs.writeFileSync(path.join(projectDir, "node_modules", "ignored"), "junk");
    fs.writeFileSync(path.join(projectDir, ".gitignore"), ".venv/\n");

    fs.copySync(projectDir, destinationDir, {
      filter: buildProjectCopyFilter(projectDir),
    });

    expect(fs.readFileSync(path.join(destinationDir, "definitions", "foo.sqlx"), "utf8")).to.equal(
      "SELECT 1",
    );
    expect(fs.existsSync(path.join(destinationDir, ".venv"))).to.equal(false);
    expect(fs.existsSync(path.join(destinationDir, "node_modules"))).to.equal(false);
  });

  test("respects a project .gitignore, in addition to the always-ignored floor", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, ".gitignore"),
      [".venv/", "__pycache__/", "*.pyc"].join("\n"),
    );
    fs.ensureDirSync(path.join(projectDir, ".venv", "lib"));
    fs.writeFileSync(path.join(projectDir, ".venv", "lib", "mod.py"), "# stub");
    fs.ensureDirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions", "foo.sqlx"), "SELECT 1");
    fs.writeFileSync(path.join(projectDir, "foo.pyc"), "junk");
    fs.ensureDirSync(path.join(projectDir, ".git"));
    fs.ensureDirSync(path.join(projectDir, "node_modules"));

    const filter = buildProjectCopyFilter(projectDir);

    // Dataform-relevant paths are still copied.
    expect(filter(projectDir)).to.equal(true);
    expect(filter(path.join(projectDir, "definitions"))).to.equal(true);
    expect(filter(path.join(projectDir, "definitions", "foo.sqlx"))).to.equal(true);
    expect(filter(path.join(projectDir, ".gitignore"))).to.equal(true);

    // gitignore'd paths are excluded -- including the bare directory itself (not
    // just its contents), which requires correctly detecting it as a directory to
    // match a trailing-slash-only pattern like `.venv/`.
    expect(filter(path.join(projectDir, ".venv"))).to.equal(false);
    expect(filter(path.join(projectDir, ".venv", "lib", "mod.py"))).to.equal(false);
    expect(filter(path.join(projectDir, "foo.pyc"))).to.equal(false);

    // The always-ignored floor still applies even when a .gitignore is present.
    expect(filter(path.join(projectDir, ".git"))).to.equal(false);
    expect(filter(path.join(projectDir, "node_modules"))).to.equal(false);
  });

  test("a .dataformignore excludes paths on its own, with no .gitignore", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".dataformignore"), "scratch/\n");
    fs.ensureDirSync(path.join(projectDir, "scratch"));
    fs.ensureDirSync(path.join(projectDir, "definitions"));

    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(path.join(projectDir, "scratch"))).to.equal(false);
    expect(filter(path.join(projectDir, "definitions"))).to.equal(true);
  });

  test("a .dataformignore supplements the .gitignore and can un-ignore its paths", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, ".gitignore"),
      [".venv/", "definitions/generated/"].join("\n"),
    );
    fs.writeFileSync(
      path.join(projectDir, ".dataformignore"),
      ["docs/", "!definitions/generated/"].join("\n"),
    );
    fs.ensureDirSync(path.join(projectDir, ".venv"));
    fs.writeFileSync(path.join(projectDir, ".venv", "ignored"), "junk");
    fs.ensureDirSync(path.join(projectDir, "docs"));
    fs.writeFileSync(path.join(projectDir, "docs", "ignored.md"), "junk");
    fs.ensureDirSync(path.join(projectDir, "definitions", "generated"));
    fs.writeFileSync(path.join(projectDir, "definitions", "generated", "gen.sqlx"), "SELECT 1");

    fs.copySync(projectDir, destinationDir, {
      filter: buildProjectCopyFilter(projectDir),
    });

    // Still excluded by the .gitignore.
    expect(fs.existsSync(path.join(destinationDir, ".venv"))).to.equal(false);
    // Excluded by the .dataformignore.
    expect(fs.existsSync(path.join(destinationDir, "docs"))).to.equal(false);
    // Gitignored, but un-ignored by the .dataformignore.
    expect(
      fs.readFileSync(path.join(destinationDir, "definitions", "generated", "gen.sqlx"), "utf8"),
    ).to.equal("SELECT 1");
  });

  test("a .dataformignore cannot override the always-ignored floor", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, "node_modules"));
    fs.writeFileSync(path.join(projectDir, ".dataformignore"), "!node_modules\n");

    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(path.join(projectDir, "node_modules"))).to.equal(false);
  });

  test("findProjectIgnoreFiles lists the ignore files present, in application order", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    expect(findProjectIgnoreFiles(projectDir)).to.deep.equal([]);

    fs.writeFileSync(path.join(projectDir, ".dataformignore"), "");
    expect(findProjectIgnoreFiles(projectDir)).to.deep.equal([".dataformignore"]);

    fs.writeFileSync(path.join(projectDir, ".gitignore"), "");
    expect(findProjectIgnoreFiles(projectDir)).to.deep.equal([".gitignore", ".dataformignore"]);
  });

  test("matches ignore patterns case-sensitively", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "definitions/staging/\n");
    fs.ensureDirSync(path.join(projectDir, "definitions", "Staging"));
    fs.writeFileSync(path.join(projectDir, "definitions", "Staging", "table.sqlx"), "SELECT 1");

    fs.copySync(projectDir, destinationDir, {
      filter: buildProjectCopyFilter(projectDir),
    });

    expect(
      fs.readFileSync(path.join(destinationDir, "definitions", "Staging", "table.sqlx"), "utf8"),
    ).to.equal("SELECT 1");
  });

  test("un-ignoring only a file does not re-include it while its directory is ignored", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "definitions/generated/\n");
    fs.writeFileSync(path.join(projectDir, ".dataformignore"), "!definitions/generated/gen.sqlx\n");
    fs.ensureDirSync(path.join(projectDir, "definitions", "generated"));
    fs.writeFileSync(path.join(projectDir, "definitions", "generated", "gen.sqlx"), "SELECT 1");

    fs.copySync(projectDir, destinationDir, {
      filter: buildProjectCopyFilter(projectDir),
    });

    // Matches git: the excluded parent directory must itself be un-ignored.
    expect(fs.existsSync(path.join(destinationDir, "definitions", "generated"))).to.equal(false);
  });

  test("the always-ignored floor matches names case-insensitively", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, ".GIT"));
    fs.ensureDirSync(path.join(projectDir, "definitions", "NODE_MODULES"));
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "!.GIT\n!NODE_MODULES\n");

    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(path.join(projectDir, ".GIT"))).to.equal(false);
    expect(filter(path.join(projectDir, "definitions", "NODE_MODULES"))).to.equal(false);
  });

  test("workflow_settings.yaml is always copied, even when an ignore file matches it", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "*.yaml\n");
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "defaultProject: p\n");
    fs.writeFileSync(path.join(projectDir, "other.yaml"), "junk");

    fs.copySync(projectDir, destinationDir, {
      filter: buildProjectCopyFilter(projectDir),
    });

    expect(fs.readFileSync(path.join(destinationDir, "workflow_settings.yaml"), "utf8")).to.equal(
      "defaultProject: p\n",
    );
    expect(fs.existsSync(path.join(destinationDir, "other.yaml"))).to.equal(false);
  });

  test("an ignore file name that is a directory is skipped, not read", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, ".dataformignore"));
    fs.writeFileSync(path.join(projectDir, ".gitignore"), ".venv/\n");
    fs.ensureDirSync(path.join(projectDir, ".venv"));

    expect(findProjectIgnoreFiles(projectDir)).to.deep.equal([".gitignore"]);
    const filter = buildProjectCopyFilter(projectDir);
    expect(filter(path.join(projectDir, ".venv"))).to.equal(false);
  });
});
