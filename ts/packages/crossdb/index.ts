import { session } from "@dataform/core";

export function currentTimestampUTC(warehouse?: string): string {
  return {
    "bigquery": "current_timestamp()",
    "redshift": "current_timestamp::timestamp",
    "snowflake": "convert_timezone('UTC', current_timestamp)::timestamp"
  }[warehouse || session.config.warehouse];
}
