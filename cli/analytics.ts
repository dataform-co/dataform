import {
  getConfigSettings,
  getConfigSettingsPath,
  upsertConfigSettings
} from "@dataform/cli/config";
import { question, ynQuestion } from "@dataform/cli/console";
import Analytics = require("analytics-node");
import { readFile, stat } from "fs";
import { promisify } from "util";
import { v4 as uuidv4 } from "uuid";

const analytics = new Analytics("eR24ln3MniE3TKZXkvAkOGkiSN02xXqw");

export async function maybeConfigureAnalytics() {
  const settings = await getConfigSettings();
  // We should only ask if users want to track analytics if they are in an interactive terminal;
  if (!process.stdout.isTTY) {
    return;
  }
  if (settings.allowAnonymousAnalytics !== undefined) {
    return;
  }
  const optInResponse = ynQuestion(
    `
To help improve the quality of our products, we collect anonymized usage data and anonymized stacktraces when crashes are encountered.
This can be changed at any point by modifying your settings file: ${getConfigSettingsPath()}

Would you like to opt-in to anonymous usage and error tracking?`,
    false
  );
  await upsertConfigSettings({
    allowAnonymousAnalytics: optInResponse,
    anonymousUserId: uuidv4()
  });
}

export async function trackCommand(command: string) {
  const config = await getConfigSettings();
  if (!config.allowAnonymousAnalytics) {
    return;
  }
  await new Promise(resolve => {
    analytics.track(
      {
        userId: config.anonymousUserId,
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
        userId: config.anonymousUserId,
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
