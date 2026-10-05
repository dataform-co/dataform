import { execFile, ExecFileOptions } from "child_process";
import * as fs from "fs-extra";
import * as http from "http";
import { dump as dumpYaml, load as loadYaml } from "js-yaml";
import * as path from "path";

import { credentials } from "df/cli/api";
import { Logger } from "df/cli/console";
import { version } from "df/core/version";
import { dataform } from "df/protos/ts";
import {
  corePackageTarPath,
  getProcessResult,
  Hook,
  nodePath,
  npmPath,
  Suite,
  writeDefinitionFile,
} from "df/testing";
import { TmpDirFixture } from "df/testing/fixtures";

const DEFAULT_PROJECT = "dataform-open-source";
const DEFAULT_LOCATION = "US";
const ADC_FILENAME = "application_default_credentials.json";
const DEFAULT_METADATA_HOST = "169.254.169.254";
const METADATA_TOKEN_PATH = "/computeMetadata/v1/instance/service-accounts/default/token";
const DEFAULT_METADATA_PROBE_TIMEOUT_MS = 1000;

interface ITestCredentialsConfig {
  projectId?: string;
  location?: string;
  credentials?: string;
}

export interface IAdcCheckOptions {
  credentialsPath?: string;
  env?: NodeJS.ProcessEnv;
  platform?: NodeJS.Platform;
  metadataProbeTimeoutMs?: number;
}

const runfilesDir = process.env.RUNFILES || "";
const workspaceName = fs.existsSync(path.resolve(runfilesDir, "df")) ? "df" : "_main";
const runfilesWorkspaceRoot = path.resolve(runfilesDir, workspaceName);

/**
 * Bridges `CLOUDSDK_CONFIG` to `GOOGLE_APPLICATION_CREDENTIALS` when the latter is unset,
 * because `google-auth-library` in Node.js only checks the default `~/.config/gcloud` path.
 */
export function bridgeCloudSdkConfig(env: NodeJS.ProcessEnv = process.env): void {
  if (!env.GOOGLE_APPLICATION_CREDENTIALS && env.CLOUDSDK_CONFIG) {
    const cloudsdkAdcPath = path.join(env.CLOUDSDK_CONFIG, ADC_FILENAME);
    if (fs.existsSync(cloudsdkAdcPath)) {
      env.GOOGLE_APPLICATION_CREDENTIALS = cloudsdkAdcPath;
    }
  }
}

bridgeCloudSdkConfig();

/**
 * Resolves the integration test credentials file path according to priority:
 * 1. `DATAFORM_TEST_CREDENTIALS` environment variable
 * 2. `test_credentials/bigquery.local.json` (git-ignored local override)
 * 3. `test_credentials/bigquery.json` (committed repository default)
 */
export function resolveCredentialsPath(
  env: NodeJS.ProcessEnv = process.env,
  baseDir: string = runfilesWorkspaceRoot,
): string {
  if (env.DATAFORM_TEST_CREDENTIALS) {
    const resolvedFromCwd = path.resolve(env.DATAFORM_TEST_CREDENTIALS);
    if (fs.existsSync(resolvedFromCwd)) {
      return resolvedFromCwd;
    }
    const resolvedFromBaseDir = path.resolve(baseDir, env.DATAFORM_TEST_CREDENTIALS);
    if (fs.existsSync(resolvedFromBaseDir)) {
      return resolvedFromBaseDir;
    }
    return resolvedFromCwd;
  }
  const localCredentialsPath = path.resolve(baseDir, "test_credentials/bigquery.local.json");
  if (fs.existsSync(localCredentialsPath)) {
    return localCredentialsPath;
  }
  return path.resolve(baseDir, "test_credentials/bigquery.json");
}

export const CREDENTIALS_PATH = resolveCredentialsPath();

function readTestCredentialsConfig(
  credentialsPath: string = CREDENTIALS_PATH,
): ITestCredentialsConfig | undefined {
  try {
    if (fs.existsSync(credentialsPath)) {
      return JSON.parse(fs.readFileSync(credentialsPath, "utf8")) as ITestCredentialsConfig;
    }
  } catch {
    // Fall back to defaults during module initialization; credentials.read() validates on use.
  }
  return undefined;
}

const logger = new Logger(true);
const initialCredentialsConfig = readTestCredentialsConfig();

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

function isMetadataServerAvailable(
  timeoutMs: number,
  env: NodeJS.ProcessEnv = process.env,
): Promise<boolean> {
  if (env.METADATA_SERVER_DETECTION?.trim().toLowerCase() === "none") {
    return Promise.resolve(false);
  }
  const metadataHost = env.GCE_METADATA_IP || env.GCE_METADATA_HOST || DEFAULT_METADATA_HOST;
  const cleanHost = metadataHost.replace(/^https?:\/\//, "");
  return new Promise<boolean>((resolve) => {
    const req = http.get(
      {
        host: cleanHost,
        path: METADATA_TOKEN_PATH,
        headers: { "Metadata-Flavor": "Google" },
        timeout: timeoutMs,
      },
      (res) => {
        res.resume();
        resolve(res.statusCode === 200 && res.headers["metadata-flavor"] === "Google");
      },
    );
    req.on("timeout", () => {
      req.destroy();
      resolve(false);
    });
    req.on("error", () => {
      resolve(false);
    });
  });
}

let defaultAdcCheckPromise: Promise<void> | null = null;

/**
 * Verifies that Application Default Credentials (or an explicit service account key in the
 * Dataform credentials JSON file) are available. Fails immediately with an actionable message
 * instead of hanging on unreachable GCE metadata server probes.
 */
export function ensureAdcAvailable(
  optionsOrPath: string | IAdcCheckOptions = CREDENTIALS_PATH,
): Promise<void> {
  const options: IAdcCheckOptions =
    typeof optionsOrPath === "string" ? { credentialsPath: optionsOrPath } : optionsOrPath;
  const credentialsPath = options.credentialsPath ?? CREDENTIALS_PATH;
  const env = options.env ?? process.env;
  const platform = options.platform ?? process.platform;
  const metadataProbeTimeoutMs =
    options.metadataProbeTimeoutMs ?? DEFAULT_METADATA_PROBE_TIMEOUT_MS;

  const isDefaultInvocation =
    credentialsPath === CREDENTIALS_PATH &&
    env === process.env &&
    platform === process.platform &&
    metadataProbeTimeoutMs === DEFAULT_METADATA_PROBE_TIMEOUT_MS;

  if (isDefaultInvocation && defaultAdcCheckPromise) {
    return defaultAdcCheckPromise;
  }

  const check = (async () => {
    // 1. Check if the Dataform credentials file contains an explicit service account key.
    const config = readTestCredentialsConfig(credentialsPath);
    if (config?.credentials?.trim()) {
      return;
    }

    // 2. Check GOOGLE_APPLICATION_CREDENTIALS environment variable.
    const envCredsPath = env.GOOGLE_APPLICATION_CREDENTIALS || env.google_application_credentials;
    if (envCredsPath) {
      if (fs.existsSync(envCredsPath)) {
        return;
      }
      throw new Error(
        `No Application Default Credentials found: GOOGLE_APPLICATION_CREDENTIALS points to a non-existent file '${envCredsPath}'. ` +
          "Run `gcloud auth application-default login` or set GOOGLE_APPLICATION_CREDENTIALS to a valid file.",
      );
    }

    // 3. Check well-known gcloud ADC file locations.
    const wellKnownPaths: string[] = [];
    if (env.CLOUDSDK_CONFIG) {
      wellKnownPaths.push(path.join(env.CLOUDSDK_CONFIG, ADC_FILENAME));
    }
    if (platform === "win32" && env.APPDATA) {
      wellKnownPaths.push(path.join(env.APPDATA, "gcloud", ADC_FILENAME));
    } else if (env.HOME) {
      wellKnownPaths.push(path.join(env.HOME, ".config", "gcloud", ADC_FILENAME));
    }
    if (wellKnownPaths.some((candidate) => fs.existsSync(candidate))) {
      return;
    }

    // 4. Probe the GCE / Cloud Build metadata server with a strict timeout.
    if (await isMetadataServerAvailable(metadataProbeTimeoutMs, env)) {
      return;
    }

    throw new Error(
      "No Application Default Credentials found. " +
        "Run `gcloud auth application-default login` or set GOOGLE_APPLICATION_CREDENTIALS.",
    );
  })();

  if (isDefaultInvocation) {
    defaultAdcCheckPromise = check;
  }
  return check;
}

/**
 * Reads the integration test credentials and registers a suite setup hook that fails fast
 * if Application Default Credentials are missing.
 */
export function getIntegrationTestCredentials(): dataform.IBigQuery {
  if (Suite.globalStack.length > 0) {
    const currentSuite = Suite.globalStack[Suite.globalStack.length - 1];
    currentSuite.prependSetUp(
      Hook.create("verify Application Default Credentials", () => ensureAdcAvailable()),
    );
  }
  return credentials.read(CREDENTIALS_PATH);
}

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

function usesDefaultTestCredentials(options: string[]): boolean {
  return options.some(
    (option) => option === CREDENTIALS_PATH || option === `--credentials=${CREDENTIALS_PATH}`,
  );
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
  if (usesDefaultTestCredentials(options)) {
    await ensureAdcAvailable(CREDENTIALS_PATH);
  }
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
