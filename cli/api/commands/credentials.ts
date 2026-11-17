import * as fs from "fs";

import * as dbadapters from "df/cli/api/dbadapters";
import { prettyJsonStringify } from "df/cli/api/utils";
import { verifyObjectMatchesProto } from "df/common/protos";
import { dataform } from "df/protos/ts";

export const CREDENTIALS_FILENAME = ".df-credentials.json";

// The file can hold a service account key, so only its owner may read or write it.
const CREDENTIALS_FILE_MODE = 0o600;

/**
 * Writes the credentials file with owner-only permissions. The mode passed when opening
 * the file only applies if that creates it, so the permissions of an existing file are
 * tightened too, before the credentials are written to it.
 */
export function write(credentialsPath: string, credentials: dataform.IBigQuery): void {
  const fileDescriptor = fs.openSync(credentialsPath, "w", CREDENTIALS_FILE_MODE);
  try {
    fs.fchmodSync(fileDescriptor, CREDENTIALS_FILE_MODE);
    fs.writeFileSync(fileDescriptor, prettyJsonStringify(credentials));
  } finally {
    fs.closeSync(fileDescriptor);
  }
}

export function read(credentialsPath: string): dataform.IBigQuery {
  if (!fs.existsSync(credentialsPath)) {
    throw new Error(`Missing credentials JSON file; not found at path '${credentialsPath}'.`);
  }
  let credentialsAsJson: object;
  try {
    credentialsAsJson = JSON.parse(fs.readFileSync(credentialsPath, "utf8"));
  } catch (e) {
    throw new Error(`Error reading credentials file: ${e.message}`);
  }
  const credentials = verifyObjectMatchesProto(dataform.BigQuery, credentialsAsJson);
  if (!Object.keys(credentials).find((key) => key === "projectId")?.length) {
    throw new Error(`Error reading credentials file: the projectId field is required`);
  }
  return credentials;
}

export enum TestResultStatus {
  SUCCESSFUL,
  TIMED_OUT,
  OTHER_ERROR,
}

export interface ITestResult {
  status: TestResultStatus;
  error?: Error;
}

export async function test(
  dbadapter: dbadapters.IDbAdapter,
  timeoutMs: number = 10000,
): Promise<ITestResult> {
  let timer;
  try {
    const timeout = new Promise<TestResultStatus>(
      (resolve) => (timer = setTimeout(() => resolve(TestResultStatus.TIMED_OUT), timeoutMs)),
    );
    const executeQuery = dbadapter.execute("SELECT 1 AS x").then(() => TestResultStatus.SUCCESSFUL);
    return {
      status: await Promise.race([executeQuery, timeout]),
    };
  } catch (e) {
    return {
      status: TestResultStatus.OTHER_ERROR,
      error: e,
    };
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}
