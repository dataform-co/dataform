import { expect } from "chai";

import { dataform } from "df/protos/ts";
import { suite, test, writeDefinitionFile, writeWorkflowSettingsFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import {
  coreExecutionRequestFromPath,
  runMainInVm,
  VALID_WORKFLOW_SETTINGS_YAML,
  WorkflowSettingsTemplates
} from "df/testing/run_core";

suite("extensions interface", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  function setUpProjectWithExtension() {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, WorkflowSettingsTemplates.bigquery);
    writeDefinitionFile(projectDir, "e.sqlx", `config {type: "view"}`);
    writeDefinitionFile(projectDir, "file.sqlx", "${resolve('e')}");

    return projectDir;
  }

  test("keeps regular compilation flow if unspecified", () => {
    const projectDir = setUpProjectWithExtension();
    const request = coreExecutionRequestFromPath(projectDir);

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals(["e", "file"]);
  });

  test("keeps regular compilation flow if extension is disabled", () => {
    const projectDir = setUpProjectWithExtension();
    const request = coreExecutionRequestFromPath(projectDir);
    request.compile.compileConfig.extension = {
      name: "some-extension",
      compilationMode: dataform.ExtensionCompilationMode.COMPILATION_MODE_UNSPECIFIED
    };

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals(["e", "file"]);
  });

  test("runs prologue before regular compilation", () => {
    const projectDir = setUpProjectWithExtension();
    const request = coreExecutionRequestFromPath(projectDir);
    request.compile.compileConfig.extension = {
      name: "@dataform/sample-extension",
      compilationMode: dataform.ExtensionCompilationMode.PROLOGUE
    };

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals([
      "sample-action",
      "e",
      "file"
    ]);
  });

  test("replaces regular compilation in application code mode", () => {
    const projectDir = setUpProjectWithExtension();
    const request = coreExecutionRequestFromPath(projectDir);
    request.compile.compileConfig.extension = {
      name: "@dataform/sample-extension",
      compilationMode: dataform.ExtensionCompilationMode.APPLICATION_CODE
    };

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals(["sample-action"]);
  });

  test("works in application mode without workflow settings", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeDefinitionFile(projectDir, "e.sqlx", `config {type: "view"}`);
    writeDefinitionFile(projectDir, "file.sqlx", "${resolve('e')}");

    const request = coreExecutionRequestFromPath(projectDir);
    request.compile.compileConfig.extension = {
      name: "@dataform/sample-extension",
      compilationMode: dataform.ExtensionCompilationMode.APPLICATION_CODE
    };

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals(["sample-action"]);
  });

  test("works in prologue mode without workflow settings", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeDefinitionFile(projectDir, "e.sqlx", `config {type: "view"}`);
    writeDefinitionFile(projectDir, "file.sqlx", "${resolve('e')}");

    const request = coreExecutionRequestFromPath(projectDir);
    request.compile.compileConfig.extension = {
      name: "@dataform/sample-extension",
      compilationMode: dataform.ExtensionCompilationMode.PROLOGUE
    };

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals([
      "sample-action",
      "e",
      "file"
    ]);
  });

  test("catches extension import exceptions", () => {
    const projectDir = setUpProjectWithExtension();
    const request = coreExecutionRequestFromPath(projectDir);
    request.compile.compileConfig.extension = {
      name: "does-not-exist",
      compilationMode: dataform.ExtensionCompilationMode.PROLOGUE
    };

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
    expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
      "Cannot find module"
    );
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals(["e", "file"]);
  });

  test("catches exceptions thrown from extension", () => {
    const projectDir = setUpProjectWithExtension();
    const request = coreExecutionRequestFromPath(
      projectDir,
      dataform.ProjectConfig.create({ vars: { "throw-error": "true" } })
    );
    request.compile.compileConfig.extension = {
      name: "@dataform/sample-extension",
      compilationMode: dataform.ExtensionCompilationMode.PROLOGUE
    };

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
    expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
      "throwing exception as requested!"
    );
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals(["e", "file"]);
  });

  test("persists extension compilation errors", () => {
    const projectDir = setUpProjectWithExtension();
    const request = coreExecutionRequestFromPath(
      projectDir,
      dataform.ProjectConfig.create({ vars: { "store-compile-error": "true" } })
    );
    request.compile.compileConfig.extension = {
      name: "@dataform/sample-extension",
      compilationMode: dataform.ExtensionCompilationMode.PROLOGUE
    };

    const result = runMainInVm(request);

    expect(result.compile.compiledGraph.graphErrors.compilationErrors.length).equals(1);
    expect(result.compile.compiledGraph.graphErrors.compilationErrors[0].message).contains(
      "storing compilation error as requested!"
    );
    expect(result.compile.compiledGraph.targets?.map(t => t.name)).deep.equals([
      "sample-action",
      "e",
      "file"
    ]);
  });

  test("preserveGovernanceControls propagates to compiled graph", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(projectDir, VALID_WORKFLOW_SETTINGS_YAML);
    writeDefinitionFile(
      projectDir,
      "file.sqlx",
      `
config {
  type: "table",
  preserveGovernanceControls: true
}
select 1 as a`
    );
    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.tables[0].bigquery.preserveGovernanceControls).equals(true);
  });

  test("preserveGovernanceControls in workflow_settings.yaml propagates to compiled graph", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: "project"
defaultDataset: "dataset"
preserveGovernanceControls: true`
    );
    writeDefinitionFile(
      projectDir,
      "file.sqlx",
      `
config {
  type: "table"
}
select 1 as a`
    );
    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.tables[0].bigquery.preserveGovernanceControls).equals(true);
  });

  test("preserveGovernanceControls on table overrides workflow_settings.yaml", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsFile(
      projectDir,
      `
defaultProject: "project"
defaultDataset: "dataset"
preserveGovernanceControls: true`
    );
    writeDefinitionFile(
      projectDir,
      "file.sqlx",
      `
config {
  type: "table",
  partitionBy: "somePartition",
  preserveGovernanceControls: false
}
select 1 as a`
    );
    const result = runMainInVm(coreExecutionRequestFromPath(projectDir));
    expect(result.compile.compiledGraph.graphErrors.compilationErrors).deep.equals([]);
    expect(result.compile.compiledGraph.tables[0].bigquery.preserveGovernanceControls).equals(
      false
    );
  });
});
