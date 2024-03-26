import * as fs from "fs";

import * as dbadapters from "df/cli/api/dbadapters";
import { verifyObjectMatchesProto } from "df/common/protos";
import { dataform } from "df/protos/ts";

export const CREDENTIALS_FILENAME = ".df-credentials.json";

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
  if (!Object.keys(credentials).find(key => key === "projectId")?.length) {
    throw new Error(`Error reading credentials file: the projectId field is required`);
  }
  return credentials;
}

export enum TestResultStatus {
  SUCCESSFUL,
  TIMED_OUT,
  OTHER_ERROR
}

export interface ITestResult {
  status: TestResultStatus;
  error?: Error;
}

export async function test(
  dbadapter: dbadapters.IDbAdapter,
  timeoutMs: number = 10000
): Promise<ITestResult> {
  let timer;
  try {
    const timeout = new Promise<TestResultStatus>(
      resolve => (timer = setTimeout(() => resolve(TestResultStatus.TIMED_OUT), timeoutMs))
    );
    const executeQuery = dbadapter.execute("SELECT 1 AS x").then(() => TestResultStatus.SUCCESSFUL);
    return {
      status: await Promise.race([executeQuery, timeout])
    };
  } catch (e) {
    return {
      status: TestResultStatus.OTHER_ERROR,
      error: e
    };
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}
