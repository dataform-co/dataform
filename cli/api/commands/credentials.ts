import * as fs from "fs";

import * as dbadapters from "df/cli/api/dbadapters";
import { prettyJsonStringify } from "df/cli/api/utils";
import { verifyObjectMatchesProto } from "df/common/protos";
import { dataform } from "df/protos/ts";

export const CREDENTIALS_FILENAME = ".df-credentials.json";

// The file can hold a service account key, so only its owner may read or write it.
const CREDENTIALS_FILE_MODE = 0o600;

/**
 * Writes the credentials file with owner-only permissions. The credentials go into a new
 * file that then replaces the target in one step, so a failed write leaves the previous
 * file as it was, an existing file with wider permissions is replaced, and a symbolic
 * link at the target is replaced rather than written through. On Windows the mode has no
 * effect beyond the read-only bit.
 */
export function write(credentialsPath: string, credentials: dataform.IBigQuery): void {
  const temporaryPath = `${credentialsPath}.${process.pid}.tmp`;
  try {
    fs.writeFileSync(temporaryPath, prettyJsonStringify(credentials), {
      mode: CREDENTIALS_FILE_MODE,
      flag: "wx",
    });
    fs.renameSync(temporaryPath, credentialsPath);
  } catch (e) {
    fs.rmSync(temporaryPath, { force: true });
    throw e;
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
