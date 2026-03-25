import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml } from "js-yaml";
import * as path from "path";

import { readConfigFromWorkflowSettings } from "df/cli/api/utils";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("readExtensionConfigFromWorkflowSettings", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  function readExtensionConfigFromWorkflowSettings(projectDir: string) {
    return readConfigFromWorkflowSettings(projectDir)?.extension ?? undefined;
  }

  test("returns undefined when workflow_settings.yaml does not exist", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    expect(readExtensionConfigFromWorkflowSettings(projectDir)).to.equal(undefined);
  });

  test("returns undefined when extension is not set in workflow_settings.yaml", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      dumpYaml({ defaultProject: "dataform" })
    );
    expect(readExtensionConfigFromWorkflowSettings(projectDir)).to.equal(undefined);
  });

  test("returns extension config when set in workflow_settings.yaml", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      dumpYaml({
        dataformCoreVersion: "3.0.0",
        defaultProject: "dataform",
        extension: {
          name: "test-extension",
          compilationMode: "PROLOGUE",
        },
      })
    );
    const result = readExtensionConfigFromWorkflowSettings(projectDir);
    expect(result.name).to.equal("test-extension");
    const mode = result.compilationMode as any;
    if (typeof mode === "string") {
      expect(mode).to.equal("PROLOGUE");
    } else {
      expect(mode).to.equal(dataform.ExtensionCompilationMode.PROLOGUE);
    }
  });

  test("throws error for invalid YAML", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "invalid: yaml: [");
    expect(() => readExtensionConfigFromWorkflowSettings(projectDir)).to.throw(
      "workflow_settings.yaml is not a valid YAML file"
    );
  });
});