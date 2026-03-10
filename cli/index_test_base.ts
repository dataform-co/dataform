// tslint:disable tsr-detect-non-literal-fs-filename
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import { execFile } from "child_process";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { corePackageTarPath, getProcessResult, nodePath, npmPath } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

export const DEFAULT_DATABASE = "dataform-open-source";
export const DEFAULT_LOCATION = "US";
export const DEFAULT_RESERVATION = "projects/dataform-open-source/locations/us/reservations/dataform-test";
export const CREDENTIALS_PATH = path.resolve(process.env.RUNFILES, "df/test_credentials/bigquery.json");

export const cliEntryPointPath = "cli/node_modules/@dataform/cli/bundle.js";

export async function setupJitProject(
  tmpDirFixture: TmpDirFixture,
  projectDir: string
): Promise<void> {
  const npmCacheDir = tmpDirFixture.createNewTmpDir();
  const packageJsonPath = path.join(projectDir, "package.json");

  await getProcessResult(
    execFile(nodePath, [cliEntryPointPath, "init", projectDir, DEFAULT_DATABASE, DEFAULT_LOCATION])
  );

  const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
  const workflowSettings = dataform.WorkflowSettings.create(
    loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
  );
  delete workflowSettings.dataformCoreVersion;
  fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettings));

  fs.writeFileSync(
    packageJsonPath,
    `{
  "dependencies":{
    "@dataform/core": "${version}"
  }
}`
  );
  await getProcessResult(
    execFile(npmPath, [
      "install",
      "--prefix",
      projectDir,
      "--cache",
      npmCacheDir,
      corePackageTarPath
    ])
  );

  const jitTablePath = path.join(projectDir, "definitions", "jit_table.js");
  fs.ensureFileSync(jitTablePath);
  fs.writeFileSync(
    jitTablePath,
    `publish("jit_table", {type: "table"}).jitCode(async (ctx) => { return "SELECT 1 as id"; })`
  );
}
