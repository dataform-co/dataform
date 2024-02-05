// tslint:disable tsr-detect-non-literal-fs-filename
import { expect } from "chai";
import * as fs from "fs-extra";
import { dump as dumpYaml } from "js-yaml";
import * as os from "os";
import * as path from "path";

import { ChildProcess, execFile } from "child_process";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

suite("@dataform/cli", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);
  const platformPath = os.platform() === "darwin" ? "nodejs_darwin_amd64" : "nodejs_linux_amd64";
  const nodePath = `external/${platformPath}/bin/node`;
  const cliEntryPointPath = "cli/node_modules/@dataform/cli/bundle.js";

  test("compile error when no @dataform/core package is installed", async () => {
    const projectDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(
      path.join(projectDir, "workflow_settings.yaml"),
      dumpYaml(dataform.WorkflowSettings.create({ dataformCoreVersion: version }))
    );

    const compileResult = await getProcessResult(
      execFile(nodePath, [cliEntryPointPath, "compile", projectDir])
    );

    expect(
      (await getProcessResult(execFile(nodePath, [cliEntryPointPath, "compile", projectDir])))
        .stderr
    ).contains(
      "Could not find a recent installed version of @dataform/core in the project. Check that " +
        "either `dataformCoreVersion` is specified in `workflow_settings.yaml`, or " +
        "`@dataform/core` is specified in `package.json`, then run `dataform install`."
    );
  });
});

async function getProcessResult(childProcess: ChildProcess) {
  let stderr = "";
  childProcess.stderr.pipe(process.stderr);
  childProcess.stderr.on("data", chunk => (stderr += String(chunk)));
  let stdout = "";
  childProcess.stdout.pipe(process.stdout);
  childProcess.stdout.on("data", chunk => (stdout += String(chunk)));
  const exitCode: number = await new Promise(resolve => {
    childProcess.on("close", resolve);
  });
  return { exitCode, stdout, stderr };
}
