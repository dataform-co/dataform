import { exists, mkdir, readFile, writeFile } from "fs";
import { homedir } from "os";
import { join } from "path";
import { promisify } from "util";

/**
 * These settings are stored as JSON on disk, as a result names must not be changed.
 */
export interface IConfigSettings {
  allowAnonymousAnalytics?: boolean;
  anonymousUserId?: string;
}

export function getConfigDirPath() {
  return join(homedir(), ".dataform");
}

export function getConfigSettingsPath() {
  return join(getConfigDirPath(), "settings.json");
}

export async function getConfigSettings(): Promise<IConfigSettings> {
  try {
    if (await promisify(exists)(getConfigSettingsPath())) {
      return JSON.parse(
        await promisify(readFile)(getConfigSettingsPath(), "utf8")
      ) as IConfigSettings;
    }
  } catch (e) {
    // If something goes wrong, return a default.
  }
  return {};
}

/**
 * Configuration setting and getting should be considered best effort, don't depend on values set here for critical user journeys.
 */
export async function upsertConfigSettings(config: Partial<IConfigSettings>) {
  try {
    const existingConfig = await getConfigSettings();
    const newConfig = { ...existingConfig, ...config };
    if (!(await promisify(exists)(getConfigDirPath()))) {
      await promisify(mkdir)(getConfigDirPath());
    }
    await promisify(writeFile)(getConfigSettingsPath(), `${JSON.stringify(newConfig, null, 4)}\n`);
  } catch (e) {
    // If we can't set the configuration, fail quietly.
  }
}
