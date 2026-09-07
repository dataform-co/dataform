// tslint:disable tsr-detect-non-literal-fs-filename
import { execFile, ExecFileOptions } from "child_process";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import { Logger } from "df/cli/console";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import { corePackageTarPath, getProcessResult, nodePath, npmPath } from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

const DEFAULT_PROJECT = "dataform-open-source";
const DEFAULT_LOCATION = "US";

const runfilesDir = process.env.RUNFILES;
let workspaceName = "df";
if (!fs.existsSync(path.resolve(runfilesDir, "df"))) {
  workspaceName = "_main";
}

export const CREDENTIALS_PATH = path.resolve(runfilesDir, workspaceName, "test_credentials/bigquery.json");

const logger = new Logger(true);

function getCredentialsProjectId(): string {
  try {
    if (fs.existsSync(CREDENTIALS_PATH)) {
      const parsed = JSON.parse(fs.readFileSync(CREDENTIALS_PATH, "utf8"));
      if (parsed?.projectId) {
        return parsed.projectId;
      }
    }
  } catch (e) {
    // Fall back to default
  }
  logger.log(`Project name not specified; defaulting to ${DEFAULT_PROJECT}`);
  return DEFAULT_PROJECT;
}

function getCredentialsLocation(): string {
  try {
    if (fs.existsSync(CREDENTIALS_PATH)) {
      const parsed = JSON.parse(fs.readFileSync(CREDENTIALS_PATH, "utf8"));
      if (parsed?.location) {
        return parsed.location;
      }
    }
  } catch (e) {
    // Fall back to default
  }
  logger.log(`Location not specified; defaulting to ${DEFAULT_LOCATION}`);
  return DEFAULT_LOCATION;
}

export const INTEGRATION_TEST_PROJECT = getCredentialsProjectId();
export const INTEGRATION_TEST_LOCATION = getCredentialsLocation();
export const INTEGRATION_TEST_RESERVATION = `projects/${INTEGRATION_TEST_PROJECT}/locations/${INTEGRATION_TEST_LOCATION.toLowerCase()}/reservations/dataform-test`;

export const cliEntryPointPath = "cli/node_modules/@dataform/cli/bundle.js";

export async function setupProject(
  tmpDirFixture: TmpDirFixture,
  projectDir: string,
  workflowSettingsOverrides?: Partial<dataform.IWorkflowSettings>
): Promise<string> {
  const npmCacheDir = tmpDirFixture.createNewTmpDir();
  const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
  const packageJsonPath = path.join(projectDir, "package.json");

  // Initialize a project using the CLI, don't install packages.
  await runCli("init", projectDir, [INTEGRATION_TEST_PROJECT, INTEGRATION_TEST_LOCATION]);

  // Install packages manually to get around bazel read-only sandbox issues.
  const workflowSettings = dataform.WorkflowSettings.create({
    ...loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")) as dataform.IWorkflowSettings,
    ...workflowSettingsOverrides
  });
  delete workflowSettings.dataformCoreVersion;
  fs.writeFileSync(
    workflowSettingsPath,
    dumpYaml(dataform.WorkflowSettings.toObject(workflowSettings, { enums: String }))
  );
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

  return projectDir;
}

export async function runCli(
  cmd: string,
  dir?: string,
  options: string[] = [],
  execOptions?: ExecFileOptions
): Promise<{
  exitCode: number;
  stdout: string;
  stderr: string;
}> {
  const args = [cliEntryPointPath, cmd, ...(dir !== undefined ? [dir] : []), ...options];
  return await getProcessResult(
    execFile(nodePath, args, execOptions)
  );
}

export function writeDefinitionFile(projectDir: string, filename: string, content: string): void {
  const fullPath = path.join(projectDir, "definitions", filename);
  fs.ensureFileSync(fullPath);
  fs.writeFileSync(fullPath, content);
}

export async function alterWorkflowSettings(projectDir: string, workflowSettingsOverrides: Partial<dataform.IWorkflowSettings>): Promise<void> {
  const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
  const workflowSettings = dataform.WorkflowSettings.create(
    loadYaml(fs.readFileSync(workflowSettingsPath, "utf8"))
  );
  const workflowSettingsNew = dataform.WorkflowSettings.create({
    ...workflowSettings,
    ...workflowSettingsOverrides
  });
  fs.writeFileSync(workflowSettingsPath, dumpYaml(workflowSettingsNew));
}

export async function setupJitProject(
  tmpDirFixture: TmpDirFixture,
  projectDir: string
): Promise<void> {
  await setupProject(tmpDirFixture, projectDir);
  writeDefinitionFile(
    projectDir,
    "jit_table.js",
    `publish("jit_table", {type: "table"}).jitCode(async (ctx) => { return "SELECT 1 as id"; })`
  );
}
