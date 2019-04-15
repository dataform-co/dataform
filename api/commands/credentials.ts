import * as dbadapters from "@dataform/api/dbadapters";
import { requiredWarehouseProps, WarehouseType } from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import * as fs from "fs";

export const CREDENTIALS_FILENAME = ".df-credentials.json";

export type Credentials = dataform.IBigQuery | dataform.IJDBC | dataform.ISnowflake;

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
    case WarehouseType.SNOWFLAKE: {
      return validateAnyAsCredentials(
        credentials,
        dataform.Snowflake.verify,
        dataform.Snowflake.create,
        requiredWarehouseProps[warehouse]
      );
    }
    default:
      throw new Error(`Unrecognized warehouse: ${warehouse}`);
  }
}

export async function test(credentials: Credentials, warehouse: string) {
  let timer;
  try {
    const timeoutSeconds = 10;
    const timeout = new Promise(
      (resolve, reject) =>
        (timer = setTimeout(
          () => reject(new Error(`Test query timed out after ${timeoutSeconds} seconds.`)),
          timeoutSeconds * 1000
        ))
    );
    await Promise.race([
      dbadapters.create(credentials, warehouse).execute("SELECT 1 AS x"),
      timeout
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function validateAnyAsCredentials<T>(
  credentials: any,
  verify: (any) => string,
  create: (any) => T,
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
