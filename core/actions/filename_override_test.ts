// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import { asPlainObject, suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML
} from "df/testing/run_core";

suite("filename_override", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("publish with filename override", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      publish("override_table", {
        type: "table",
        filename: "custom/path/to/file.sql"
      }).query("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const table = result.compile.compiledGraph.tables.find(
      t => t.target.name === "override_table"
    );
    expect(table.fileName).equals("custom/path/to/file.sql");
  });

  test("publish with omitted filename dynamically determined", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      publish("dynamic_table", {
        type: "table"
      }).query("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const table = result.compile.compiledGraph.tables.find(t => t.target.name === "dynamic_table");
    expect(table.fileName).equals("definitions/file.js");
  });


  test("incremental with filename override", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      publish("override_incremental", {
        type: "incremental",
        filename: "custom/path/to/incremental.sql"
      }).query("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const table = result.compile.compiledGraph.tables.find(
      t => t.target.name === "override_incremental"
    );
    expect(table.fileName).equals("custom/path/to/incremental.sql");
  });

  test("incremental with omitted filename dynamically determined", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      publish("dynamic_incremental", {
        type: "incremental"
      }).query("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const table = result.compile.compiledGraph.tables.find(
      t => t.target.name === "dynamic_incremental"
    );
    expect(table.fileName).equals("definitions/file.js");
  });

  test("operate with filename override", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      operate("override_op", {
        filename: "custom/path/to/op.sql"
      }).queries("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const op = result.compile.compiledGraph.operations.find(
      o => o.target.name === "override_op"
    );
    expect(op.fileName).equals("custom/path/to/op.sql");
  });

  test("operate with omitted filename dynamically determined", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      operate("dynamic_op").queries("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const op = result.compile.compiledGraph.operations.find(o => o.target.name === "dynamic_op");
    expect(op.fileName).equals("definitions/file.js");
  });


  test("assert with filename override", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      assert("override_assert", {
        filename: "custom/path/to/assert.sql"
      }).query("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const assertion = result.compile.compiledGraph.assertions.find(
      a => a.target.name === "override_assert"
    );
    expect(assertion.fileName).equals("custom/path/to/assert.sql");
  });

  test("assert with omitted filename dynamically determined", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      assert("dynamic_assert").query("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const assertion = result.compile.compiledGraph.assertions.find(
      a => a.target.name === "dynamic_assert"
    );
    expect(assertion.fileName).equals("definitions/file.js");
  });

  test("declare with filename override", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      declare({
        database: "db",
        schema: "schema",
        name: "override_decl",
        filename: "custom/path/to/decl.sql"
      });
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const decl = result.compile.compiledGraph.declarations.find(
      d => d.target.name === "override_decl"
    );
    expect(decl.fileName).equals("custom/path/to/decl.sql");
  });

  test("declare with omitted filename dynamically determined", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      declare({
        database: "db",
        schema: "schema",
        name: "dynamic_decl"
      });
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const decl = result.compile.compiledGraph.declarations.find(
      d => d.target.name === "dynamic_decl"
    );
    expect(decl.fileName).equals("definitions/file.js");
  });

  test("test with filename override", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      test("override_test")
        .config({
          filename: "custom/path/to/test.sql",
          dataset: "override_table"
        })
        .input("override_table", "SELECT 1")
        .expect("SELECT 1");
      
      publish("override_table").query("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const t = result.compile.compiledGraph.tests.find(
      testProto => testProto.name === "override_test"
    );
    expect(t.fileName).equals("custom/path/to/test.sql");
  });

  test("test with omitted filename dynamically determined", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      test("dynamic_test")
        .config({
          dataset: "override_table"
        })
        .input("override_table", "SELECT 1")
        .expect("SELECT 1");
      
      publish("override_table").query("SELECT 1");
      `
    );

    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const t = result.compile.compiledGraph.tests.find(
      testProto => testProto.name === "dynamic_test"
    );
    expect(t.fileName).equals("definitions/file.js");
  });

  test("notebook with filename explicitly set", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      VALID_WORKFLOW_SETTINGS_YAML
    );
    fs.mkdirSync(path.join(projectDir, "definitions"));
    fs.writeFileSync(path.join(projectDir, "definitions/notebook.ipynb"), '{"cells":[]}');
    
    fs.writeFileSync(
      path.join(projectDir, "definitions/file.js"),
      `
      notebook({
        name: "override_notebook",
        location: "US",
        filename: "notebook.ipynb" // This is used to load content, it MUST point to valid notebook.
      });
      `
    );
    
    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    const nb = result.compile.compiledGraph.notebooks.find(
      n => n.target.name === "override_notebook"
    );
    // It should use the explicitly provided filename
    expect(nb.fileName).to.contain("definitions/notebook.ipynb");
  });
});
