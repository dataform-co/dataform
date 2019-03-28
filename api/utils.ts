import { requiredWarehouseProps, WarehouseTypes } from "@dataform/core/adapters";
import { dataform } from "@dataform/protos";
import * as fs from "fs";

export type Credentials = dataform.IBigQuery | dataform.IJDBC | dataform.ISnowflake;

export function readCredentials(warehouse: string, credentialsPath: string): Credentials {
  if (!fs.existsSync(credentialsPath)) {
    throw new Error("Missing credentials JSON file.");
  }
  return coerceCredentials(warehouse, JSON.parse(fs.readFileSync(credentialsPath, "utf8")));
}

export function coerceCredentials(warehouse: string, credentials: any): Credentials {
  switch (warehouse) {
    case WarehouseTypes.BIGQUERY: {
      return validateAnyAsCredentials(
        credentials,
        dataform.BigQuery.verify,
        dataform.BigQuery.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseTypes.REDSHIFT: {
      return validateAnyAsCredentials(
        credentials,
        dataform.JDBC.verify,
        dataform.JDBC.create,
        requiredWarehouseProps[warehouse]
      );
    }
    case WarehouseTypes.SNOWFLAKE: {
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
