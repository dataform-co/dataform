import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml } from "js-yaml";
import * as path from "path";

import { compile } from "df/cli/vm/compile";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

/**
 * Covers compile-time backward-compat shims that let a newer CLI drive an
 * older @dataform/core. Runs `compile()` in-process against a hand-rolled fake
 * @dataform/core bundle whose main() reports back what globals it observed —
 * so we can prove a shim ran (or didn't) without npm-installing an old core.
 */

interface IFakeCoreObservation {
  hasLineage: boolean;
  version: string;
}

function setupFakeCoreProject(
  projectDir: string,
  coreVersion: string,
  workflowSettings: { [key: string]: any }
): void {
  fs.writeFileSync(
    path.join(projectDir, "workflow_settings.yaml"),
    dumpYaml(workflowSettings)
  );

  const coreDir = path.join(projectDir, "node_modules", "@dataform", "core");
  fs.mkdirSync(coreDir, { recursive: true });
  fs.writeFileSync(
    path.join(coreDir, "package.json"),
    JSON.stringify({ name: "@dataform/core", version: coreVersion, main: "bundle.js" })
  );

  // Fake bundle: newer-style YAML compiler (module.exports = <parsed>) so the
  // shim's `global.workflowSettingsYaml.lineage` access mirrors the real path
  // in modern @dataform/core; main() serializes what it observes into a JSON
  // string so the test can assert on the return value.
  const parsedSettingsJson = JSON.stringify(workflowSettings);
  const bundleJs = `
    module.exports = {
      version: ${JSON.stringify(coreVersion)},
      compiler: function(code, filePath) {
        if (filePath && (filePath.endsWith("workflow_settings.yaml") ||
                         filePath.endsWith("workflow_settings.yml"))) {
          return "module.exports = " + ${JSON.stringify(parsedSettingsJson)} + ";";
        }
        return code;
      },
      main: function(_base64Request) {
        return JSON.stringify({
          hasLineage: !!(global.workflowSettingsYaml && global.workflowSettingsYaml.lineage),
          version: ${JSON.stringify(coreVersion)}
        });
      }
    };
  `;
  fs.writeFileSync(path.join(coreDir, "bundle.js"), bundleJs);
}

function runCompileAgainstFakeCore(projectDir: string): IFakeCoreObservation {
  const result = compile(
    dataform.CompileConfig.create({ projectDir })
  );
  return JSON.parse(result as unknown as string);
}

suite("compile backwards compatibility shims", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test("lineage block is stripped from workflow_settings for @dataform/core < 3.0.60", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    setupFakeCoreProject(projectDir, "3.0.50", {
      defaultProject: "test-project",
      defaultLocation: "US",
      lineage: { enabled: true }
    });

    const observed = runCompileAgainstFakeCore(projectDir);

    expect(observed.version).to.equal("3.0.50");
    expect(observed.hasLineage).to.equal(false);
  });

  test("lineage block passes through at @dataform/core == 3.0.60 threshold", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    setupFakeCoreProject(projectDir, "3.0.60", {
      defaultProject: "test-project",
      defaultLocation: "US",
      lineage: { enabled: true }
    });

    const observed = runCompileAgainstFakeCore(projectDir);

    expect(observed.version).to.equal("3.0.60");
    expect(observed.hasLineage).to.equal(true);
  });

  test("lineage block passes through for @dataform/core > 3.0.60", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    setupFakeCoreProject(projectDir, "3.0.61", {
      defaultProject: "test-project",
      defaultLocation: "US",
      lineage: { enabled: true }
    });

    const observed = runCompileAgainstFakeCore(projectDir);

    expect(observed.version).to.equal("3.0.61");
    expect(observed.hasLineage).to.equal(true);
  });
});
