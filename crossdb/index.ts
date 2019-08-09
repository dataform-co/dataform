export function currentTimestampUTC(warehouse?: string): string {
  return ({
    bigquery: "current_timestamp()",
    redshift: "current_timestamp::timestamp",
    snowflake: "convert_timezone('UTC', current_timestamp())::timestamp",
    sqldatawarehouse: "CURRENT_TIMESTAMP"
  } as { [key: string]: string })[warehouse || (global as any).session.config.warehouse];
}
