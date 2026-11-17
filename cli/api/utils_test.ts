import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml } from "js-yaml";
import * as path from "path";

import { readConfigFromWorkflowSettings, validateDataformCoreVersion } from "df/cli/api/utils";
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
      dumpYaml({ defaultProject: "dataform" }),
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
      }),
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
      "workflow_settings.yaml is not a valid YAML file",
    );
  });
});

suite("validateDataformCoreVersion", () => {
  for (const validVersion of [
    "3.0.0",
    "3.1.5",
    "0.0.1",
    "3.0.0-beta.1",
    "2.0.0-alpha.1.2",
    "3.1.0-rc-1",
  ]) {
    test(`accepts valid semantic version "${validVersion}"`, () => {
      expect(() => validateDataformCoreVersion(validVersion)).to.not.throw();
    });
  }

  for (const invalidVersion of [
    "",
    "   ",
    " 3.0.0",
    "3.0.0\n",
    "3.0",
    "v3.0.0",
    "03.0.0",
    "3.01.0",
    "3.0.01",
    "3.0.0-01",
    "3.0.0-.",
    "3.0.0-..",
    "3.0.0-.alpha",
    "3.0.0-alpha.",
    "3.0.0-alpha..1",
    "^3.0.0",
    "~3.0.0",
    ">=3.0.0",
    "*",
    "latest",
    "3.x",
    "3.0.0-",
    "3.0.0+build.1",
    "https://example.invalid/core.tgz",
    "http://example.invalid/core.tgz",
    "git+https://example.invalid/repo.git",
    "file:./local-core.tgz",
    "npm:semver@7.5.4",
    "../other",
    "3.0.0 --loglevel=http",
  ]) {
    test(`rejects invalid string specifier ${JSON.stringify(invalidVersion)}`, () => {
      expect(() => validateDataformCoreVersion(invalidVersion)).to.throw(
        `Invalid dataformCoreVersion ${JSON.stringify(invalidVersion)}: must be a valid semantic version.`,
      );
    });
  }

  for (const { label, value, expectedFormatted } of [
    { label: "array", value: ["3.0.0"], expectedFormatted: '["3.0.0"]' },
    { label: "float", value: 3.1, expectedFormatted: "3.1" },
    { label: "integer", value: 300, expectedFormatted: "300" },
    { label: "NaN", value: NaN, expectedFormatted: "NaN" },
    { label: "Infinity", value: Infinity, expectedFormatted: "Infinity" },
    { label: "BigInt", value: BigInt(1), expectedFormatted: "1" },
    { label: "boolean", value: true, expectedFormatted: "true" },
    { label: "null", value: null, expectedFormatted: "null" },
    { label: "undefined", value: undefined, expectedFormatted: "undefined" },
    {
      label: "object",
      value: { version: "3.0.0" },
      expectedFormatted: '{"version":"3.0.0"}',
    },
  ]) {
    test(`rejects non-string ${label} value`, () => {
      expect(() => validateDataformCoreVersion(value)).to.throw(
        `Invalid dataformCoreVersion ${expectedFormatted}: must be a valid semantic version.`,
      );
    });
  }

  test("rejects circular object without throwing TypeError", () => {
    const circular: unknown[] = [];
    circular.push(circular);

    expect(() => validateDataformCoreVersion(circular)).to.throw(
      /Invalid dataformCoreVersion .*: must be a valid semantic version\./,
    );
  });
});

suite("readConfigFromWorkflowSettings dataformCoreVersion validation", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  for (const validVersion of ["3.0.0", "3.1.5", "3.0.0-beta.1"]) {
    test(`reads valid dataformCoreVersion "${validVersion}"`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `defaultProject: dataform\ndataformCoreVersion: ${validVersion}\n`,
      );

      expect(readConfigFromWorkflowSettings(projectDir)?.dataformCoreVersion).to.equal(
        validVersion,
      );
    });
  }

  for (const { label, yamlContent } of [
    { label: "omitted dataformCoreVersion", yamlContent: "defaultProject: dataform\n" },
    {
      label: "null dataformCoreVersion",
      yamlContent: "defaultProject: dataform\ndataformCoreVersion:\n",
    },
    {
      label: "empty string dataformCoreVersion",
      yamlContent: 'defaultProject: dataform\ndataformCoreVersion: ""\n',
    },
    { label: "empty workflow_settings.yaml", yamlContent: "" },
  ]) {
    test(`treats ${label} as unset`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), yamlContent);

      expect(readConfigFromWorkflowSettings(projectDir)?.dataformCoreVersion).to.equal("");
    });
  }

  for (const { label, yamlLine, expectedError } of [
    {
      label: "HTTPS URL",
      yamlLine: 'dataformCoreVersion: "https://example.invalid/core.tgz"',
      expectedError:
        'Invalid dataformCoreVersion "https://example.invalid/core.tgz": must be a valid semantic version.',
    },
    {
      label: "file tarball path",
      yamlLine: 'dataformCoreVersion: "file:./local-core.tgz"',
      expectedError:
        'Invalid dataformCoreVersion "file:./local-core.tgz": must be a valid semantic version.',
    },
    {
      label: "git URL",
      yamlLine: 'dataformCoreVersion: "git+https://example.invalid/repo.git"',
      expectedError:
        'Invalid dataformCoreVersion "git+https://example.invalid/repo.git": must be a valid semantic version.',
    },
    {
      label: "semver range",
      yamlLine: 'dataformCoreVersion: "^3.0.0"',
      expectedError: 'Invalid dataformCoreVersion "^3.0.0": must be a valid semantic version.',
    },
    {
      label: "YAML array",
      yamlLine: 'dataformCoreVersion: ["3.0.0"]',
      expectedError: 'Invalid dataformCoreVersion ["3.0.0"]: must be a valid semantic version.',
    },
    {
      label: "YAML float",
      yamlLine: "dataformCoreVersion: 3.1",
      expectedError: "Invalid dataformCoreVersion 3.1: must be a valid semantic version.",
    },
    {
      label: "YAML boolean",
      yamlLine: "dataformCoreVersion: true",
      expectedError: "Invalid dataformCoreVersion true: must be a valid semantic version.",
    },
    {
      label: "leading zero version",
      yamlLine: 'dataformCoreVersion: "03.0.0"',
      expectedError: 'Invalid dataformCoreVersion "03.0.0": must be a valid semantic version.',
    },
    {
      label: "malformed dot prerelease",
      yamlLine: 'dataformCoreVersion: "3.0.0-."',
      expectedError: 'Invalid dataformCoreVersion "3.0.0-.": must be a valid semantic version.',
    },
    {
      label: "whitespace string",
      yamlLine: 'dataformCoreVersion: "   "',
      expectedError: 'Invalid dataformCoreVersion "   ": must be a valid semantic version.',
    },
    {
      label: "YAML NaN",
      yamlLine: "dataformCoreVersion: .nan",
      expectedError: "Invalid dataformCoreVersion NaN: must be a valid semantic version.",
    },
    {
      label: "YAML Infinity",
      yamlLine: "dataformCoreVersion: .inf",
      expectedError: "Invalid dataformCoreVersion Infinity: must be a valid semantic version.",
    },
  ]) {
    test(`rejects ${label} in workflow_settings.yaml`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        `defaultProject: dataform\n${yamlLine}\n`,
      );

      expect(() => readConfigFromWorkflowSettings(projectDir)).to.throw(expectedError);
    });
  }

  test("rejects circular YAML alias in workflow_settings.yaml without TypeError", () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      "defaultProject: dataform\ndataformCoreVersion: &a [*a]\n",
    );

    expect(() => readConfigFromWorkflowSettings(projectDir)).to.throw(
      /Invalid dataformCoreVersion .*: must be a valid semantic version\./,
    );
  });
});
