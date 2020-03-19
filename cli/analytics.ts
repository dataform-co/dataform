import {
  getConfigSettings,
  getConfigSettingsPath,
  upsertConfigSettings
} from "@dataform/cli/config";
import { question } from "@dataform/cli/console";
import Analytics = require("analytics-node");
import { readFile, stat } from "fs";
import { promisify } from "util";
import { v4 as uuidv4 } from "uuid";

const analytics = new Analytics("eR24ln3MniE3TKZXkvAkOGkiSN02xXqw");

export async function maybeConfigureAnalytics() {
  const settings = await getConfigSettings();
  const dockerStatus = await getDockerStatus();
  // We should only ask if users want to track analytics if they aren't in docker, e.g. for CI.
  if (!dockerStatus && settings.allowAnonymousAnalytics === undefined) {
    const optInResponse = question(
      `
To help improve the quality of our products, we collect anonymized usage data and anonymized stacktraces when crashes are encountered.
This can be changed at any point by modifying the file: ${getConfigSettingsPath()}

Do you want to opt in to anonymous usage and error tracking (y/N)?`
    );
    await upsertConfigSettings({
      allowAnonymousAnalytics: optInResponse.toLowerCase() === "y",
      userId: uuidv4()
    });
  }
}

export async function trackCommand(command: string) {
  const config = await getConfigSettings();
  if (!config.allowAnonymousAnalytics) {
    return;
  }
  // Don't block execution on the result of the analytics call.
  await new Promise(resolve => {
    analytics.track(
      {
        userId: config.userId,
        event: "event_dataform_cli_command",
        properties: {
          command
        }
      },
      e => {
        resolve();
        // Silently fail on tracking errors.
      }
    );
  });
}

export async function trackError(e: any) {
  const config = await getConfigSettings();
  if (!config.allowAnonymousAnalytics) {
    return;
  }
  await new Promise(resolve => {
    analytics.track(
      {
        userId: config.userId,
        event: "event_dataform_cli_error",
        properties: {
          message: e && e.message ? e.message : String(e)
        }
      },
      () => {
        resolve();
        // Silently fail on tracking errors.
      }
    );
  });
}

async function getDockerStatus() {
  const checks = Promise.all([
    (async () => {
      try {
        await promisify(stat)("/.dockerenv");
        return true;
      } catch (_) {
        return false;
      }
    })(),
    (async () => {
      try {
        const cgroups = await promisify(readFile)("/proc/self/cgroup", "utf8");
        return cgroups.includes("docker");
      } catch (_) {
        return false;
      }
    })()
  ]);
  return (await checks).some(v => v);
}
