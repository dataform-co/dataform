import yargs from "yargs";

import { Analytics}  from "@segment/analytics-node";
import {getConfigSettings, getConfigSettingsPath, upsertConfigSettings} from "df/cli/config";
import {ynQuestion} from "df/cli/console";
import {INamedOption} from "df/cli/yargswrapper";
import {v4 as uuidv4} from "uuid";

export const trackOption: INamedOption<yargs.Options> = {
  name: "track",
  option: {
    describe: `Sets analytics tracking without asking the user. Overrides settings.json`,
    type: "boolean"
  }
};

const analytics = new Analytics({ writeKey: "eR24ln3MniE3TKZXkvAkOGkiSN02xXqw"});

let currentCommand: string;
let allowAnonymousAnalytics: boolean;
let anonymousUserId: string;

export async function maybeConfigureAnalytics(track?: boolean) {
  const settings = await getConfigSettings();
  if (track !== undefined) {
    allowAnonymousAnalytics = track;
    if (track) {
      // in the case where the user *wants* tracking and has no settings.json
      // assign them a temporary tracking id
      anonymousUserId = settings.anonymousUserId || uuidv4();
    }
    return;
  }
  // We should only ask if users want to track analytics if they are in an interactive terminal;
  if (!process.stdout.isTTY) {
    return;
  }
  if (settings.allowAnonymousAnalytics !== undefined) {
    allowAnonymousAnalytics = settings.allowAnonymousAnalytics;
    anonymousUserId = settings.anonymousUserId;
    return;
  }

  const optInResponse = ynQuestion(
    `
To help improve the quality of our products, we collect anonymized usage data and anonymized stacktraces when crashes are encountered.
This can be changed at any point by modifying your settings file: ${getConfigSettingsPath()}

Would you like to opt-in to anonymous usage and error tracking?`,
    false
  );
  allowAnonymousAnalytics = optInResponse;
  anonymousUserId = uuidv4();

  await upsertConfigSettings({
    allowAnonymousAnalytics,
    anonymousUserId
  });
}

export async function trackCommand(command: string) {
  if (!allowAnonymousAnalytics) {
    return;
  }
  currentCommand = command;
  await new Promise(resolve => {
    analytics.track(
      {
        userId: anonymousUserId,
        event: "event_dataform_cli_command",
        properties: {
          command
        }
      },
      () => {
        resolve();
        // Silently fail on tracking errors.
      }
    );
  });
}

export async function trackError() {
  if (!allowAnonymousAnalytics) {
    return;
  }
  await new Promise(resolve => {
    analytics.track(
      {
        userId: anonymousUserId,
        event: "event_dataform_cli_error",
        properties: {
          currentCommand
        }
      },
      () => {
        resolve();
        // Silently fail on tracking errors.
      }
    );
  });
}
