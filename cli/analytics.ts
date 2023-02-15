import yargs from "yargs";

import Analytics from "analytics-node";
import {getConfigSettings, getConfigSettingsPath, upsertConfigSettings} from "df/cli/config";
import {ynQuestion} from "df/cli/console";
import {INamedOption} from "df/cli/yargswrapper";
import {v4 as uuidv4} from "uuid";

export const noTrackOption: INamedOption<yargs.Options> = {
  name: "no-track",
  option: {
    describe: `Disables analytics without asking the user. A settings.json file will be written at ${getConfigSettingsPath()} to store this setting`,
    type: "boolean",
    default: false
  }
};

const analytics = new Analytics("eR24ln3MniE3TKZXkvAkOGkiSN02xXqw");

let currentCommand: string;
let allowAnonymousAnalytics: boolean;

export async function maybeConfigureAnalytics(noTrack: boolean) {
  const settings = await getConfigSettings();
  // We should only ask if users want to track analytics if they are in an interactive terminal;
  if (!process.stdout.isTTY) {
    return;
  }
  if (settings.allowAnonymousAnalytics !== undefined) {
    allowAnonymousAnalytics = settings.allowAnonymousAnalytics;
    return;
  }
  if (noTrack) {
    allowAnonymousAnalytics = false;
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
  allowAnonymousAnalytics = optInResponse;
}

export async function trackCommand(command: string) {
  currentCommand = command;
  const config = await getConfigSettings();
  if (!allowAnonymousAnalytics) {
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
      () => {
        resolve();
        // Silently fail on tracking errors.
      }
    );
  });
}

export async function trackError() {
  const config = await getConfigSettings();
  if (!allowAnonymousAnalytics) {
    return;
  }
  await new Promise(resolve => {
    analytics.track(
      {
        userId: config.anonymousUserId,
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
