import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import {
  buildProjectCopyFilter,
  copyProjectForStatelessInstall,
  explainExcludedModule,
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
    fs.ensureDirSync(path.join(projectDir, "scratch", "nested", "node_modules"));
    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(projectDir)).to.equal(true);
    expect(filter(path.join(projectDir, "definitions"))).to.equal(true);
    expect(filter(path.join(projectDir, "definitions", "foo.sqlx"))).to.equal(true);
    expect(filter(path.join(projectDir, ".venv"))).to.equal(true);

    expect(filter(path.join(projectDir, ".git"))).to.equal(false);
    expect(filter(path.join(projectDir, "node_modules"))).to.equal(false);
    // Excluded at any depth, not just at the project root.
    expect(filter(path.join(projectDir, "scratch", "nested", "node_modules"))).to.equal(false);
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

  test("definitions/ and includes/ are copied even when the .gitignore matches them", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, ".gitignore"),
      ["definitions/generated/", "*.js", "includes/", "scratch/"].join("\n"),
    );
    fs.ensureDirSync(path.join(projectDir, "definitions", "generated"));
    fs.writeFileSync(path.join(projectDir, "definitions", "generated", "gen.sqlx"), "SELECT 1");
    fs.writeFileSync(path.join(projectDir, "definitions", "actions.js"), "// actions");
    fs.ensureDirSync(path.join(projectDir, "includes"));
    fs.writeFileSync(path.join(projectDir, "includes", "constants.js"), "// constants");
    fs.ensureDirSync(path.join(projectDir, "scratch"));
    fs.writeFileSync(path.join(projectDir, "scratch", "notes.js"), "// notes");
    fs.writeFileSync(path.join(projectDir, "helper.js"), "// helper");

    fs.copySync(projectDir, destinationDir, {
      filter: buildProjectCopyFilter(projectDir),
    });

    expect(
      fs.readFileSync(path.join(destinationDir, "definitions", "generated", "gen.sqlx"), "utf8"),
    ).to.equal("SELECT 1");
    expect(fs.existsSync(path.join(destinationDir, "definitions", "actions.js"))).to.equal(true);
    expect(fs.existsSync(path.join(destinationDir, "includes", "constants.js"))).to.equal(true);
    // The .gitignore still applies outside them.
    expect(fs.existsSync(path.join(destinationDir, "scratch"))).to.equal(false);
    expect(fs.existsSync(path.join(destinationDir, "helper.js"))).to.equal(false);
  });

  test("inside definitions/ and includes/, node_modules is copied but .git is not", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    // Compilation's glob descends into nested node_modules but skips dot-directories.
    fs.ensureDirSync(path.join(projectDir, "definitions", "vendored", "node_modules"));
    fs.writeFileSync(
      path.join(projectDir, "definitions", "vendored", "node_modules", "table.sqlx"),
      "SELECT 1",
    );
    fs.ensureDirSync(path.join(projectDir, "includes", ".git"));

    const filter = buildProjectCopyFilter(projectDir);

    expect(
      filter(path.join(projectDir, "definitions", "vendored", "node_modules", "table.sqlx")),
    ).to.equal(true);
    expect(filter(path.join(projectDir, "includes", ".git"))).to.equal(false);
  });

  test("copies gitignored paths that definitions/ reaches through a symlink", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "generated/\n");
    fs.ensureDirSync(path.join(projectDir, "generated", "tables"));
    fs.writeFileSync(path.join(projectDir, "generated", "tables", "table.sqlx"), "SELECT 1");
    fs.symlinkSync("generated", path.join(projectDir, "definitions"));

    copyProjectForStatelessInstall(projectDir, destinationDir);

    expect(
      fs.readFileSync(path.join(destinationDir, "definitions", "tables", "table.sqlx"), "utf8"),
    ).to.equal("SELECT 1");
  });

  test("follows symlinks nested in includes/, chained links and their parents", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "build/\nlinks/\n");
    fs.ensureDirSync(path.join(projectDir, "build", "js"));
    fs.writeFileSync(path.join(projectDir, "build", "js", "constants.js"), "// constants");
    fs.writeFileSync(path.join(projectDir, "build", "unrelated.txt"), "junk");
    // includes/constants.js -> ../links/js/constants.js, where links -> build.
    fs.symlinkSync("build", path.join(projectDir, "links"));
    fs.ensureDirSync(path.join(projectDir, "includes"));
    fs.symlinkSync(
      path.join("..", "links", "js", "constants.js"),
      path.join(projectDir, "includes", "constants.js"),
    );

    copyProjectForStatelessInstall(projectDir, destinationDir);

    expect(fs.readFileSync(path.join(destinationDir, "includes", "constants.js"), "utf8")).to.equal(
      "// constants",
    );
    // Only what the link needs is kept; the rest of the ignored directory isn't.
    expect(fs.existsSync(path.join(destinationDir, "build", "unrelated.txt"))).to.equal(false);
  });

  test("a symlink to the project root makes the whole project a compilation input", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "generated/\n");
    fs.ensureDirSync(path.join(projectDir, "generated"));
    fs.writeFileSync(path.join(projectDir, "generated", "table.sqlx"), "SELECT 1");
    fs.ensureDirSync(path.join(projectDir, ".git"));
    fs.writeFileSync(path.join(projectDir, ".git", "HEAD"), "ref: refs/heads/main\n");
    // Compilation's glob follows this, so it compiles definitions/generated/table.sqlx.
    fs.symlinkSync(".", path.join(projectDir, "definitions"));

    copyProjectForStatelessInstall(projectDir, destinationDir);

    expect(fs.readFileSync(path.join(destinationDir, "generated", "table.sqlx"), "utf8")).to.equal(
      "SELECT 1",
    );
    expect(fs.existsSync(path.join(destinationDir, ".git"))).to.equal(false);
  });

  test("terminates on symlink loops and tolerates dangling links", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, "definitions"));
    fs.symlinkSync("..", path.join(projectDir, "definitions", "parent"));
    fs.symlinkSync("loop-b", path.join(projectDir, "definitions", "loop-a"));
    fs.symlinkSync("loop-a", path.join(projectDir, "definitions", "loop-b"));
    fs.symlinkSync("missing", path.join(projectDir, "definitions", "dangling"));

    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(path.join(projectDir, "definitions", "loop-a"))).to.equal(true);
    expect(filter(path.join(projectDir, "definitions", "dangling"))).to.equal(true);
  });

  test("does not follow symlinks out of the project", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const outsideDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(outsideDir, "big"));
    fs.ensureDirSync(path.join(projectDir, "definitions"));
    fs.symlinkSync(outsideDir, path.join(projectDir, "definitions", "outside"));

    const filter = buildProjectCopyFilter(projectDir);

    // The link itself is copied as a link; nothing outside the project is walked.
    expect(filter(path.join(projectDir, "definitions", "outside"))).to.equal(true);
  });

  test("on a case-sensitive filesystem, patterns only match their exact case", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "scratch/\n");
    fs.ensureDirSync(path.join(projectDir, "Scratch"));
    fs.writeFileSync(path.join(projectDir, "Scratch", "notes.txt"), "keep");

    fs.copySync(projectDir, destinationDir, {
      filter: buildProjectCopyFilter(projectDir, false),
    });

    expect(fs.readFileSync(path.join(destinationDir, "Scratch", "notes.txt"), "utf8")).to.equal(
      "keep",
    );
  });

  test("on a case-insensitive filesystem, patterns and negations ignore case", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "scratch/\n*.txt\n!Keep.txt\n");
    fs.ensureDirSync(path.join(projectDir, "Scratch"));
    fs.writeFileSync(path.join(projectDir, "keep.txt"), "");
    fs.writeFileSync(path.join(projectDir, "drop.txt"), "");

    const filter = buildProjectCopyFilter(projectDir, true);

    expect(filter(path.join(projectDir, "Scratch"))).to.equal(false);
    expect(filter(path.join(projectDir, "keep.txt"))).to.equal(true);
    expect(filter(path.join(projectDir, "drop.txt"))).to.equal(false);
  });

  test("on a case-sensitive filesystem, negations only match their exact case", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "*.txt\n!Keep.txt\n");
    fs.writeFileSync(path.join(projectDir, "keep.txt"), "");
    fs.writeFileSync(path.join(projectDir, "Keep.txt"), "");

    const filter = buildProjectCopyFilter(projectDir, false);

    expect(filter(path.join(projectDir, "Keep.txt"))).to.equal(true);
    expect(filter(path.join(projectDir, "keep.txt"))).to.equal(false);
  });

  test("the always-ignored floor follows the filesystem's case sensitivity", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, ".GIT"));
    fs.ensureDirSync(path.join(projectDir, "scratch", "NODE_MODULES"));
    fs.writeFileSync(path.join(projectDir, "scratch", "NODE_MODULES", "file.txt"), "");
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "!.GIT\n!NODE_MODULES\n");

    // Case-insensitive: these are the excluded directories, negations notwithstanding.
    const insensitiveFilter = buildProjectCopyFilter(projectDir, true);
    expect(insensitiveFilter(path.join(projectDir, ".GIT"))).to.equal(false);
    expect(insensitiveFilter(path.join(projectDir, "scratch", "NODE_MODULES"))).to.equal(false);

    // Case-sensitive: these are ordinary directories.
    const sensitiveFilter = buildProjectCopyFilter(projectDir, false);
    expect(sensitiveFilter(path.join(projectDir, ".GIT"))).to.equal(true);
    expect(sensitiveFilter(path.join(projectDir, "scratch", "NODE_MODULES", "file.txt"))).to.equal(
      true,
    );
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

  test(".npmrc is always copied, even when the .gitignore matches it", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), ".npmrc\n");
    fs.writeFileSync(path.join(projectDir, ".npmrc"), "registry=https://example.com/\n");

    fs.copySync(projectDir, destinationDir, {
      filter: buildProjectCopyFilter(projectDir),
    });

    expect(fs.readFileSync(path.join(destinationDir, ".npmrc"), "utf8")).to.equal(
      "registry=https://example.com/\n",
    );
  });

  test("findProjectIgnoreFiles lists only ignore files in the project root, in order", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    expect(findProjectIgnoreFiles(projectDir)).deep.equals([]);

    fs.ensureDirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions", ".gitignore"), "");
    fs.writeFileSync(path.join(projectDir, "definitions", ".dataformignore"), "");
    expect(findProjectIgnoreFiles(projectDir)).deep.equals([]);

    fs.writeFileSync(path.join(projectDir, ".dataformignore"), "");
    expect(findProjectIgnoreFiles(projectDir)).deep.equals([".dataformignore"]);

    fs.writeFileSync(path.join(projectDir, ".gitignore"), "");
    expect(findProjectIgnoreFiles(projectDir)).deep.equals([".gitignore", ".dataformignore"]);
  });

  test(".dataformignore re-includes gitignored inputs that aren't compilation inputs", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "queries/\n.certs/\n*.pem\n");
    fs.writeFileSync(path.join(projectDir, ".dataformignore"), "!/queries/\n!/.certs/\n!*.pem\n");
    // Generated SQL that definitions/actions.yaml references, and the CA file .npmrc
    // points to: both are needed in the copy, but neither is a compilation input.
    fs.ensureDirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions", "actions.yaml"),
      "actions:\n  - table:\n      filename: ../queries/example.sql\n",
    );
    fs.ensureDirSync(path.join(projectDir, "queries"));
    fs.writeFileSync(path.join(projectDir, "queries", "example.sql"), "SELECT 1");
    fs.writeFileSync(path.join(projectDir, ".npmrc"), "cafile=.certs/company-ca.pem\n");
    fs.ensureDirSync(path.join(projectDir, ".certs"));
    fs.writeFileSync(path.join(projectDir, ".certs", "company-ca.pem"), "certificate");

    copyProjectForStatelessInstall(projectDir, destinationDir);

    expect(fs.readFileSync(path.join(destinationDir, "queries", "example.sql"), "utf8")).to.equal(
      "SELECT 1",
    );
    expect(fs.readFileSync(path.join(destinationDir, ".certs", "company-ca.pem"), "utf8")).to.equal(
      "certificate",
    );
  });

  test(".dataformignore can exclude more, but not compilation inputs or the fixed floor", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, ".dataformignore"),
      ["scratch/", "definitions/", "includes/", "!.git/", "!node_modules/"].join("\n"),
    );
    fs.ensureDirSync(path.join(projectDir, "scratch"));
    fs.ensureDirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions", "foo.sqlx"), "SELECT 1");
    fs.ensureDirSync(path.join(projectDir, "includes"));
    fs.ensureDirSync(path.join(projectDir, ".git"));
    fs.ensureDirSync(path.join(projectDir, "node_modules"));
    const filter = buildProjectCopyFilter(projectDir);

    expect(filter(path.join(projectDir, "scratch"))).to.equal(false);
    expect(filter(path.join(projectDir, "definitions"))).to.equal(true);
    expect(filter(path.join(projectDir, "definitions", "foo.sqlx"))).to.equal(true);
    expect(filter(path.join(projectDir, "includes"))).to.equal(true);
    expect(filter(path.join(projectDir, ".git"))).to.equal(false);
    expect(filter(path.join(projectDir, "node_modules"))).to.equal(false);
  });

  test(".dataformignore can't re-include a file while its parent directory is excluded", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const destinationDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "queries/\n");
    fs.writeFileSync(path.join(projectDir, ".dataformignore"), "!queries/example.sql\n");
    fs.ensureDirSync(path.join(projectDir, "queries"));
    fs.writeFileSync(path.join(projectDir, "queries", "example.sql"), "SELECT 1");

    copyProjectForStatelessInstall(projectDir, destinationDir);

    // As in git: the copy never descends into the excluded directory.
    expect(fs.existsSync(path.join(destinationDir, "queries"))).to.equal(false);
  });

  test("a .gitignore that is a directory is skipped, not read", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.ensureDirSync(path.join(projectDir, ".gitignore"));
    fs.ensureDirSync(path.join(projectDir, ".venv"));

    expect(findProjectIgnoreFiles(projectDir)).deep.equals([]);
    const filter = buildProjectCopyFilter(projectDir);
    expect(filter(path.join(projectDir, ".venv"))).to.equal(true);
  });

  test("on a case-insensitive filesystem, workflow_settings.yaml is kept in any case", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "*.yaml\n");
    fs.writeFileSync(path.join(projectDir, "Workflow_Settings.yaml"), "defaultProject: p\n");

    expect(
      buildProjectCopyFilter(projectDir, true)(path.join(projectDir, "Workflow_Settings.yaml")),
    ).to.equal(true);
    expect(
      buildProjectCopyFilter(projectDir, false)(path.join(projectDir, "Workflow_Settings.yaml")),
    ).to.equal(false);
  });
});

suite("explainExcludedModule", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  function setUpCopy(): { projectDir: string; copyDir: string } {
    const projectDir = tmpDirFixture.createNewTmpDir();
    const copyDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, ".gitignore"), "generated/\n");
    fs.ensureDirSync(path.join(projectDir, "generated", "queries"));
    fs.writeFileSync(path.join(projectDir, "generated", "queries", "example.sql"), "SELECT 1");
    fs.ensureDirSync(path.join(projectDir, "lib"));
    fs.writeFileSync(path.join(projectDir, "lib", "helper.js"), "");
    copyProjectForStatelessInstall(projectDir, copyDir);
    return { projectDir, copyDir };
  }

  test("names the shallowest excluded path for a module the copy filter dropped", () => {
    const { projectDir, copyDir } = setUpCopy();
    const message = "Cannot find module 'generated/queries/example.sql'";

    expect(explainExcludedModule(message, projectDir, copyDir)).to.equal(
      `${message}. It exists in the project, but an ignore file excludes 'generated/' from ` +
        "the copy compiled for dataformCoreVersion. To include it, add '!/generated/' to " +
        ".dataformignore",
    );
    // An absolute path inside the copy is explained the same way.
    expect(
      explainExcludedModule(
        `Cannot find module '${path.join(copyDir, "generated", "queries", "example.sql")}'`,
        projectDir,
        copyDir,
      ),
    ).to.contain("excludes 'generated/'");
  });

  test("leaves other missing modules unexplained", () => {
    const { projectDir, copyDir } = setUpCopy();
    for (const message of [
      // A genuinely missing npm dependency.
      "Cannot find module 'lodash'",
      // Copied, so not the filter's doing.
      "Cannot find module 'lib/helper.js'",
      // Resolved against the requiring file, which the message doesn't name.
      "Cannot find module './example.sql'",
      // Missing from the project too.
      "Cannot find module 'generated/missing.sql'",
      // Not a missing module at all.
      "Unexpected token",
    ]) {
      expect(explainExcludedModule(message, projectDir, copyDir)).to.equal(message);
    }
  });
});
