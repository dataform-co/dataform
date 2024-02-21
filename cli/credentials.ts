import * as fs from "fs";

import { question, selectionQuestion } from "df/cli/console";
import { actuallyResolve } from "df/cli/util";
import { dataform } from "df/protos/ts";

export function getBigQueryCredentials(): dataform.IBigQuery {
  const locationIndex = selectionQuestion("Enter the location of your datasets:", [
    "US (default)",
    "EU",
    "other"
  ]);
  let location = locationIndex === 0 ? "US" : "EU";
  if (locationIndex === 2) {
    location = question("Enter the location's region name (e.g. 'asia-south1'):");
  }
  const isApplicationDefaultOrJSONKeyIndex = selectionQuestion(
    "Do you wish to use Application Default Credentials or JSON Key:",
    ["ADC (default)", "JSON Key"]
  );
  if (isApplicationDefaultOrJSONKeyIndex === 0) {
    const projectId = question("Enter your billing project ID:");
    return {
      projectId,
      location
    };
  }
  const cloudCredentialsPath = actuallyResolve(
    question(
      "Please follow the instructions at https://docs.dataform.co/dataform-cli#create-a-credentials-file/\n" +
        "to create and download a private key from the Google Cloud Console in JSON format.\n" +
        "(You can delete this file after credential initialization is complete.)\n\n" +
        "Enter the path to your Google Cloud private key file:"
    )
  );
  if (!fs.existsSync(cloudCredentialsPath)) {
    throw new Error(`Google Cloud private key file "${cloudCredentialsPath}" does not exist!`);
  }
  const cloudCredentials = JSON.parse(fs.readFileSync(cloudCredentialsPath, "utf8"));
  return {
    projectId: cloudCredentials.project_id,
    credentials: fs.readFileSync(cloudCredentialsPath, "utf8"),
    location
  };
}
