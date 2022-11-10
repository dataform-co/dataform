import * as fs from "fs";

import * as dbadapters from "df/api/dbadapters";
import { requiredWarehouseProps, WarehouseType } from "df/core/adapters";
import { dataform } from "df/protos/ts";

export const CREDENTIALS_FILENAME = ".df-credentials.json";

export type Credentials =
  | dataform.IBigQuery
  | dataform.IJDBC
  | dataform.IPresto
  | dataform.ISnowflake
  | dataform.ISQLDataWarehouse;

export function read(warehouse: string, credentialsPath: string): Credentials {
  if (!fs.existsSync(credentialsPath)) {
    throw new Error("Missing credentials JSON file.");
  }
  return coerce(warehouse, JSON.parse(fs.readFileSync(credentialsPath, "utf8")));
}

export function coerce(warehouse: string, credentials: any): Credentials {
  switch (warehouse) {
    case WarehouseType.BIGQUERY: {
      return validateAnyAsCredentials(
        credentials,
        dataform.BigQuery.verify,
        dataform.BigQuery.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseType.POSTGRES:
    case WarehouseType.REDSHIFT: {
      return validateAnyAsCredentials(
        credentials,
        dataform.JDBC.verify,
        dataform.JDBC.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseType.PRESTO: {
      return validateAnyAsCredentials(
        credentials,
        dataform.Presto.verify,
        dataform.Presto.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseType.SNOWFLAKE: {
      return validateAnyAsCredentials(
        credentials,
        dataform.Snowflake.verify,
        dataform.Snowflake.create,
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
