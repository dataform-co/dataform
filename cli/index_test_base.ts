import { execFile, ExecFileOptions } from "child_process";
import * as fs from "fs-extra";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import { Logger } from "df/cli/console";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import {
  corePackageTarPath,
  getProcessResult,
  nodePath,
  npmPath,
  writeDefinitionFile,
} from "df/testing";
import { readTestCredentialsConfig, resolveCredentialsPath } from "df/testing/credentials";
import { TmpDirFixture } from "df/testing/fixtures";

const DEFAULT_PROJECT = "dataform-open-source";
const DEFAULT_LOCATION = "US";

export const CREDENTIALS_PATH = resolveCredentialsPath();

const logger = new Logger(true);
const initialCredentialsConfig = readTestCredentialsConfig(CREDENTIALS_PATH);

function getCredentialsProjectId(): string {
  if (initialCredentialsConfig?.projectId) {
    return initialCredentialsConfig.projectId;
  }
  logger.log(`Project name not specified; defaulting to ${DEFAULT_PROJECT}`);
  return DEFAULT_PROJECT;
}

function getCredentialsLocation(): string {
  if (initialCredentialsConfig?.location) {
    return initialCredentialsConfig.location;
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
  workflowSettingsOverrides?: Partial<dataform.IWorkflowSettings>,
): Promise<string> {
  const npmCacheDir = tmpDirFixture.createNewTmpDir();
  const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
  const packageJsonPath = path.join(projectDir, "package.json");

  // Initialize a project using the CLI, don't install packages.
  await runCli("init", [projectDir, INTEGRATION_TEST_PROJECT, INTEGRATION_TEST_LOCATION]);

  // Install packages manually to get around bazel read-only sandbox issues.
  const workflowSettings = dataform.WorkflowSettings.create({
    ...(loadYaml(fs.readFileSync(workflowSettingsPath, "utf8")) as dataform.IWorkflowSettings),
    ...workflowSettingsOverrides,
  });
  delete workflowSettings.dataformCoreVersion;
  fs.writeFileSync(
    workflowSettingsPath,
    dumpYaml(dataform.WorkflowSettings.toObject(workflowSettings, { enums: String })),
  );
  fs.writeFileSync(
    packageJsonPath,
    `{
  "dependencies":{
    "@dataform/core": "${version}"
  }
}`,
  );
  await getProcessResult(
    execFile(npmPath, [
      "install",
      "--prefix",
      projectDir,
      "--cache",
      npmCacheDir,
      corePackageTarPath,
    ]),
  );

  return projectDir;
}

export async function runCli(
  cmd: string,
  options: string[] = [],
  execOptions?: ExecFileOptions,
): Promise<{
  exitCode: number;
  stdout: string;
  stderr: string;
}> {
  const args = [cliEntryPointPath, cmd, ...options];
  return getProcessResult(execFile(nodePath, args, execOptions));
}

export function alterWorkflowSettings(
  projectDir: string,
  workflowSettingsOverrides: Partial<dataform.IWorkflowSettings>,
): void {
  const workflowSettingsPath = path.join(projectDir, "workflow_settings.yaml");
  const existingSettings = loadYaml(
    fs.readFileSync(workflowSettingsPath, "utf8"),
  ) as dataform.IWorkflowSettings;
  const workflowSettings = dataform.WorkflowSettings.create({
    ...existingSettings,
    ...workflowSettingsOverrides,
  });
  fs.writeFileSync(
    workflowSettingsPath,
    dumpYaml(dataform.WorkflowSettings.toObject(workflowSettings, { enums: String })),
  );
}

export async function setupJitProject(
  tmpDirFixture: TmpDirFixture,
  projectDir: string,
): Promise<void> {
  await setupProject(tmpDirFixture, projectDir);
  writeDefinitionFile(
    projectDir,
    "jit_table.js",
    `publish("jit_table", {type: "table"}).jitCode(async (ctx) => { return "SELECT 1 as id"; })`,
  );
}
