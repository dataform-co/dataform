import * as fs from "fs-extra";
import * as http from "http";
import * as https from "https";
import * as path from "path";

import { IHookHandler } from "df/testing";

// Helpers for integration test credentials and Application Default Credentials (ADC).
// This module intentionally has no import-time side effects: nothing here reads or modifies the
// environment until one of its functions is called.

const ADC_FILENAME = "application_default_credentials.json";
const DEFAULT_METADATA_HOST = "169.254.169.254";
const METADATA_TOKEN_PATH = "/computeMetadata/v1/instance/service-accounts/default/token";
const DEFAULT_METADATA_PROBE_TIMEOUT_MS = 1000;

export interface ITestCredentialsConfig {
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

/** Returns the root of the main repository inside the Bazel runfiles tree. */
export function runfilesWorkspaceRoot(env: NodeJS.ProcessEnv = process.env): string {
  const runfilesDir = env.RUNFILES || "";
  const workspaceName = fs.existsSync(path.resolve(runfilesDir, "df")) ? "df" : "_main";
  return path.resolve(runfilesDir, workspaceName);
}

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

/**
 * Resolves the integration test credentials file: the git-ignored local override
 * `test_credentials/bigquery.json` if present, otherwise the committed default
 * `test_credentials/bigquery.default.json`.
 */
export function resolveCredentialsPath(baseDir: string = runfilesWorkspaceRoot()): string {
  const localCredentialsPath = path.resolve(baseDir, "test_credentials/bigquery.json");
  if (fs.existsSync(localCredentialsPath)) {
    return localCredentialsPath;
  }
  return path.resolve(baseDir, "test_credentials/bigquery.default.json");
}

/** Reads the credentials JSON file, returning undefined if it is missing or malformed. */
export function readTestCredentialsConfig(
  credentialsPath: string,
): ITestCredentialsConfig | undefined {
  try {
    if (fs.existsSync(credentialsPath)) {
      return JSON.parse(fs.readFileSync(credentialsPath, "utf8")) as ITestCredentialsConfig;
    }
  } catch {
    // Callers fall back to defaults; credentials.read() reports malformed files on use.
  }
  return undefined;
}

/**
 * Verifies that Application Default Credentials (or an explicit service account key in the
 * Dataform credentials JSON file) are available. Fails immediately with an actionable message
 * instead of hanging on unreachable GCE metadata server probes.
 */
export async function ensureAdcAvailable(options: IAdcCheckOptions = {}): Promise<void> {
  const env = options.env ?? process.env;
  const credentialsPath = options.credentialsPath ?? resolveCredentialsPath();
  const platform = options.platform ?? process.platform;
  const metadataProbeTimeoutMs =
    options.metadataProbeTimeoutMs ?? DEFAULT_METADATA_PROBE_TIMEOUT_MS;

  // 1. Check if the Dataform credentials file contains an explicit service account key.
  const config = readTestCredentialsConfig(credentialsPath);
  if (config?.credentials?.trim()) {
    return;
  }

  // 2. Check GOOGLE_APPLICATION_CREDENTIALS environment variable.
  const envCredsPath = env.GOOGLE_APPLICATION_CREDENTIALS;
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
}

/**
 * Registers a suite set-up hook that prepares and verifies Application Default Credentials for
 * integration tests, failing the whole suite fast with an actionable message when none are
 * available. Call it first in the suite body, so that it runs before any other set-up hook:
 *
 *   suite("...", ({ before }) => {
 *     requireAdc(before);
 *     ...
 *   });
 */
export function requireAdc(setUp: IHookHandler, options: IAdcCheckOptions = {}): void {
  setUp("verify Application Default Credentials", async () => {
    // Done here, explicitly, rather than at import time: this mutates the environment, which is
    // read later by google-auth-library and inherited by child processes (e.g. the CLI).
    bridgeCloudSdkConfig(options.env ?? process.env);
    await ensureAdcAvailable(options);
  });
}

function isMetadataServerAvailable(timeoutMs: number, env: NodeJS.ProcessEnv): Promise<boolean> {
  if (env.METADATA_SERVER_DETECTION?.trim().toLowerCase() === "none") {
    return Promise.resolve(false);
  }
  const metadataHost = env.GCE_METADATA_IP || env.GCE_METADATA_HOST || DEFAULT_METADATA_HOST;
  return new Promise<boolean>((resolve) => {
    try {
      // Same as gcp-metadata: keep an explicit http(s):// scheme, default to http.
      const baseUrl = /^https?:\/\//.test(metadataHost) ? metadataHost : `http://${metadataHost}`;
      const url = new URL(METADATA_TOKEN_PATH, baseUrl);
      const options = { headers: { "Metadata-Flavor": "Google" }, timeout: timeoutMs };
      const onResponse = (res: http.IncomingMessage) => {
        res.resume();
        resolve(res.statusCode === 200 && res.headers["metadata-flavor"] === "Google");
      };
      const req =
        url.protocol === "https:"
          ? https.get(url, options, onResponse)
          : http.get(url, options, onResponse);
      req.on("timeout", () => {
        req.destroy();
        resolve(false);
      });
      req.on("error", () => {
        resolve(false);
      });
    } catch {
      // A malformed GCE_METADATA_HOST makes the URL invalid; treat it as an unavailable server.
      resolve(false);
    }
  });
}
