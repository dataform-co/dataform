import { suite, test } from "df/testing";
import { dataform } from "df/protos/ts";
import * as path from "path";
import * as fs from "fs-extra";
import * as main from "df/core/main";
import { compile } from "df/core/compilers";
import { TmpDirFixture } from "df/testing/fixtures";
import { expect } from "chai";
import { CompilerFunction, NodeVM } from "vm2";
import { decode64, encode64 } from "df/common/protos";

const VALID_WORKFLOW_SETTINGS_YAML = `
warehouse: bigquery
defaultLocation: US
`;

const VALID_DATAFORM_JSON = `
{
  "warehouse": "bigquery"
}
`;

suite("@dataform/core", ({ beforeEach, afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  suite("workflow settings", () => {
    test(`main succeeds when a valid workflow_settings.yaml is present`, () => {
      const projectDir = tmpDirFixture.createNewTmpDir();
      fs.writeFileSync(
        path.join(projectDir, "workflow_settings.yaml"),
        VALID_WORKFLOW_SETTINGS_YAML
      );
      const coreExecutionRequest = dataform.CoreExecutionRequest.create({
        compile: { compileConfig: { projectDir } }
      });

      const result = runMainInVm(coreExecutionRequest);
      expect(result.compile.compiledGraph.projectConfig).deep.equals({
        warehouse: "bigquery",
        defaultLocation: "US"
      });
    });

    // // dataform.json for workflow settings is deprecated, but still currently supported.
    // test(`main succeeds when a valid dataform.json is present`, () => {
    //   const projectDir = tmpDirFixture.createNewTmpDir();
    //   fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);
    //   const coreExecutionRequest = dataform.CoreExecutionRequest.create({
    //     compile: { compileConfig: { projectDir } }
    //   });

    //   runMainInVm(coreExecutionRequest);
    // });

    // test(`main fails when no workflow settings file is present`, () => {
    //   const projectDir = tmpDirFixture.createNewTmpDir();
    //   const coreExecutionRequest = dataform.CoreExecutionRequest.create({
    //     compile: { compileConfig: { projectDir } }
    //   });

    //   expect(() => runMainInVm(coreExecutionRequest)).to.throw(
    //     "Failed to resolve workflow_settings.yaml"
    //   );
    // });

    // test(`main fails when both workflow settings and dataform.json files are present`, () => {
    //   const projectDir = tmpDirFixture.createNewTmpDir();
    //   fs.writeFileSync(path.join(projectDir, "dataform.json"), VALID_DATAFORM_JSON);
    //   fs.writeFileSync(
    //     path.join(projectDir, "workflow_settings.yaml"),
    //     VALID_WORKFLOW_SETTINGS_YAML
    //   );
    //   const coreExecutionRequest = dataform.CoreExecutionRequest.create({
    //     compile: { compileConfig: { projectDir } }
    //   });

    //   expect(() => runMainInVm(coreExecutionRequest)).to.throw(
    //     "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
    //   );
    // });

    // test(`main fails when workflow_settings.yaml is an invalid yaml file`, () => {
    //   const projectDir = tmpDirFixture.createNewTmpDir();
    //   fs.writeFileSync(path.join(projectDir, "workflow_settings.yaml"), "doubleColon::");
    //   const coreExecutionRequest = dataform.CoreExecutionRequest.create({
    //     compile: { compileConfig: { projectDir } }
    //   });

    //   expect(() => runMainInVm(coreExecutionRequest)).to.throw(
    //     "dataform.json has been deprecated and cannot be defined alongside workflow_settings.yaml"
    //   );
    // });

    // test(`main fails when dataform.json is an invalid json file`, () => {
    //   const projectDir = tmpDirFixture.createNewTmpDir();
    //   fs.writeFileSync(path.join(projectDir, "dataform.json"), '{keyWithNoQuotes: "validValue"}');
    //   const coreExecutionRequest = dataform.CoreExecutionRequest.create({
    //     compile: { compileConfig: { projectDir } }
    //   });

    //   expect(() => runMainInVm(coreExecutionRequest)).to.throw(
    //     "Unexpected token k in JSON at position 1"
    //   );
    // });
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
      resolve: (moduleName, parentDirName) => {
        path.join(parentDirName, path.relative(parentDirName, projectDir), moduleName);
      }
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
