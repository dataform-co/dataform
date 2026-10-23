import * as fs from "fs";

import { question, selectionQuestion } from "df/cli/console";
import { actuallyResolve } from "df/cli/util";
import { dataform } from "df/protos/ts";

export interface ICredentialsPrompts {
  question: (questionText: string) => string;
  selectionQuestion: (questionText: string, options: string[]) => number;
}

export function getBigQueryCredentials(
  prompts: ICredentialsPrompts = { question, selectionQuestion },
): dataform.IBigQuery {
  const locationIndex = prompts.selectionQuestion("Enter the location of your datasets:", [
    "US (default)",
    "EU",
    "other",
  ]);
  let location = locationIndex === 0 ? "US" : "EU";
  if (locationIndex === 2) {
    location = prompts.question("Enter the location's region name (e.g. 'asia-south1'):");
  }
  const isApplicationDefaultOrJSONKeyIndex = prompts.selectionQuestion(
    "Do you wish to use Application Default Credentials or JSON Key:",
    ["ADC (default)", "JSON Key"],
  );
  if (isApplicationDefaultOrJSONKeyIndex === 0) {
    const projectId = prompts.question("Enter your billing project ID:");
    return withServiceAccountToImpersonate({ projectId, location }, prompts);
  }
  const cloudCredentialsPath = actuallyResolve(
    prompts.question(
      "Please follow the instructions at https://docs.dataform.co/dataform-cli#create-a-credentials-file/\n" +
        "to create and download a private key from the Google Cloud Console in JSON format.\n" +
        "(You can delete this file after credential initialization is complete.)\n\n" +
        "Enter the path to your Google Cloud private key file:",
    ),
  );
  if (!fs.existsSync(cloudCredentialsPath)) {
    throw new Error(`Google Cloud private key file "${cloudCredentialsPath}" does not exist!`);
  }
  return withServiceAccountToImpersonate(
    credentialsFromServiceAccountJson(fs.readFileSync(cloudCredentialsPath, "utf8"), location),
    prompts,
  );
}

// Copy universe_domain from the key so BigQuery and google-auth target the same universe.
export function credentialsFromServiceAccountJson(
  keyJson: string,
  location: string,
): dataform.IBigQuery {
  const cloudCredentials = JSON.parse(keyJson);
  const universeDomain =
    typeof cloudCredentials.universe_domain === "string"
      ? cloudCredentials.universe_domain.trim()
      : "";
  return {
    projectId: cloudCredentials.project_id,
    credentials: keyJson,
    location,
    ...(universeDomain ? { universeDomain } : {}),
  };
}

/** Asks for an optional service account to impersonate with the given credentials. */
function withServiceAccountToImpersonate(
  credentials: dataform.IBigQuery,
  prompts: ICredentialsPrompts,
): dataform.IBigQuery {
  const impersonateServiceAccount = prompts
    .question("Enter a service account to impersonate (optional, press Enter to skip):")
    .trim();
  return impersonateServiceAccount ? { ...credentials, impersonateServiceAccount } : credentials;
}
