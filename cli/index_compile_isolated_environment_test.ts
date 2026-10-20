import { expect } from "chai";

import { runCliWithTmpDir, writeWorkflowSettingsWithCoreVersion } from "df/cli/index_test_base";
import { suite, test, writeDefinitionFile } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

// These tests install @dataform/core from the npm registry, so they run with the integration tests.
suite("compile in an isolated environment", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  test(
    "compile removes its temporary environment for dataformCoreVersion projects",
    { timeout: 60000 },
    async () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      writeWorkflowSettingsWithCoreVersion(projectDir, "3.0.71");
      writeDefinitionFile(projectDir, "example.sqlx", `config { type: "table" }\nSELECT 1 AS id`);

      const result = await runCliWithTmpDir(tmpDirFixture, "compile", [projectDir, "--json"]);

      expect(result.exitCode, `compile failed: ${result.stderr}`).equals(0);
      expect(result.leftoverEnvironments).deep.equals([]);
    },
  );

  test("compile explains a failed @dataform/core installation", { timeout: 60000 }, async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    writeWorkflowSettingsWithCoreVersion(projectDir, "3.999.0");

    const result = await runCliWithTmpDir(tmpDirFixture, "compile", [projectDir, "--json"]);

    expect(result.exitCode).equals(1);
    expect(result.stderr).contains(
      "Failed to install @dataform/core@3.999.0 (dataformCoreVersion in workflow_settings.yaml).",
    );
    expect(result.leftoverEnvironments).deep.equals([]);
  });
});
