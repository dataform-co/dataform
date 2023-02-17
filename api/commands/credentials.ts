import * as fs from "fs";

import * as dbadapters from "df/api/dbadapters";
import { requiredWarehouseProps, WarehouseType } from "df/core/adapters";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

export const CREDENTIALS_FILENAME = ".df-credentials.json";

export type Credentials =
  | profiles.BigQuery
  | profiles.JDBC
  | profiles.Presto
  | profiles.Snowflake
  | dataform.SQLDataWarehouse;

export function read(warehouse: string, credentialsPath: string): Credentials {
  if (!fs.existsSync(credentialsPath)) {
    throw new Error(`Missing credentials JSON file; not found at path '${credentialsPath}'.`);
  }
  return coerce(warehouse, JSON.parse(fs.readFileSync(credentialsPath, "utf8")));
}

export function coerce(warehouse: string, credentials: any): Credentials {
  switch (warehouse) {
    case WarehouseType.BIGQUERY: {
      return validateAnyAsCredentials(
        credentials,
        profiles.BigQuery.verify,
        profiles.BigQuery.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseType.POSTGRES:
    case WarehouseType.REDSHIFT: {
      return validateAnyAsCredentials(
        credentials,
        profiles.JDBC.verify,
        profiles.JDBC.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseType.PRESTO: {
      return validateAnyAsCredentials(
        credentials,
        profiles.Presto.verify,
        profiles.Presto.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseType.SNOWFLAKE: {
      return validateAnyAsCredentials(
        credentials,
        profiles.Snowflake.verify,
        profiles.Snowflake.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseType.SQLDATAWAREHOUSE: {
      return validateAnyAsCredentials(
        credentials,
        dataform.SQLDataWarehouse.verify,
        dataform.SQLDataWarehouse.create,
        requiredWarehouseProps[warehouse]
      );
    }
    default:
      throw new Error(`Unrecognized warehouse: ${warehouse}`);
  }
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

function validateAnyAsCredentials<T>(
  credentials: any,
  verify: (credentials: any) => string,
  create: (credentials: any) => T,
  requiredProps: string[]
): T {
  const errMsg = verify(credentials);
  if (errMsg) {
    throw new Error(`Credentials JSON object does not conform to protobuf requirements: ${errMsg}`);
  }
  const protobuf = create(credentials);
  const missingProps = requiredProps.filter(key => Object.keys(protobuf).indexOf(key) === -1);
  if (missingProps.length > 0) {
    throw new Error(`Missing required properties: ${missingProps.join(", ")}`);
  }
  return protobuf;
}
