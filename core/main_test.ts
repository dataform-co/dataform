import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";
import { CompilerFunction, NodeVM } from "vm2";

import { decode64, encode64 } from "df/common/protos";
import { compile } from "df/core/compilers";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";
import { asPlainObject } from "df/tests/utils";

const VALID_WORKFLOW_SETTINGS_YAML = `
defaultDatabase: dataform
`;

const VALID_DATAFORM_JSON = `
{
  "warehouse": "bigquery",
  "defaultDatabase": "dataform"
}
`;

suite("@dataform/core", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("workflow settings", () => {
    test(`main succeeds when a valid workflow_settings.yaml is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      const result = runMainInVm(coreExecutionRequest);

      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
        asPlainObject({
          warehouse: "bigquery",
          defaultDatabase: "dataform"
        })
      );
    });

    // dataform.json for workflow settings is deprecated, but still currently supported.
    test(`main succeeds when a valid dataform.json is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      const result = runMainInVm(coreExecutionRequest);

      expect(asPlainObject(result.compile.compiledGraph.projectConfig)).deep.equals(
        asPlainObject({
          warehouse: "bigquery",
          defaultDatabase: "dataform"
        })
      );
    });

    test(`main fails when no workflow settings file is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "Failed to resolve workflow_settings.yaml"
      );
    });

    test(`main fails when both workflow settings and dataform.json files are present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
      );
    });

    test(`main fails when workflow_settings.yaml is an invalid yaml file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "&*19132sdS:asd:");
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw("workflow_settings.yaml is invalid");
    });

    test(`main fails when dataform.json is an invalid json file`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(path.join(projectDir, "dataform.json"), '{keyWithNoQuotes: "validValue"}');
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "Unexpected token k in JSON at position 1"
      );
    });

    test(`main fails when a valid workflow_settings.yaml contains unknown fields`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        "notAProjectConfigField: value"
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "Workflow settings error: unexpected key 'notAProjectConfigField'"
      );
    });

    test(`main fails when a valid dataform.json contains unknown fields`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
      fs.writeFileSync(
        path.join(projectDir, "dataform.json"),
        `{"notAProjectConfigField": "value"}`
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      expect(() => runMainInVm(coreExecutionRequest)).to.throw(
        "Workflow settings error: unexpected key 'notAProjectConfigField'"
      );
    });

    // TODO(ekrekr): add a test for nested fields, once they exist.
  });
});

// A VM is needed when running main because Node functions like `require` are overridden.
function runMainInVm(coreExecutionRequest: dataform.CoreExecutionRequest) {
  const projectDir = coreExecutionRequest.compile.compileConfig.projectDir;

  // Copy over the build Dataform Core that is set up as a node_modules directory.
  fs.copySync(`${process.cwd()}/core/node_modules`, `${projectDir}/node_modules`);

  const compiler = compile as CompilerFunction;
  // Then use vm2's native compiler integration to apply the compiler to files.
  const nodeVm = new NodeVM({
    // Inheriting the console makes console.logs show when tests are running, which is useful for
    // debugging.
    console: "inherit",
    wrapper: "none",
    require: {
      builtin: ["path"],
      context: "sandbox",
      external: true,
      root: projectDir,
      resolve: (moduleName, parentDirName) =>
        path.join(parentDirName, path.relative(parentDirName, projectDir), moduleName)
    },
    sourceExtensions: ["js", "sql", "sqlx", "yaml"],
    compiler
  });

  const encodedCoreExecutionRequest = encode64(dataform.CoreExecutionRequest, coreExecutionRequest);
  const vmIndexFileName = path.resolve(path.join(projectDir, "index.js"));
  const encodedCoreExecutionResponse = nodeVm.run(
    `return require("@dataform/core").main("${encodedCoreExecutionRequest}")`,
    vmIndexFileName
  );
  return decode64(dataform.CoreExecutionResponse, encodedCoreExecutionResponse);
}
