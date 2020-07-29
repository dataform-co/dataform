import { ISqlDialect } from "df/sql";

export type DurationUnit = "day" | "week" | "month" | "quarter" | "year";

export class Timestamps {
  constructor(private readonly dialect: ISqlDialect) {}

  public fromMillis(timestampMillis: string) {
    if (this.dialect === "snowflake") {
      return `to_timestamp(${timestampMillis}, 3)`;
    }
    if (this.dialect === "postgres") {
      return `timestamp 'epoch' + (${timestampMillis} / 1000) * interval '1 second'`;
    }
    return `timestamp_millis(${timestampMillis.toString()})`;
  }

  public truncate(timestamp: string, timestampUnit: DurationUnit) {
    if (this.dialect === "snowflake") {
      return `date_trunc(${timestampUnit}, ${timestamp})`;
    }
    if (this.dialect === "postgres") {
      return `date_trunc('${timestampUnit}', ${timestamp})`;
    }
    return `timestamp_trunc(${timestamp}, ${timestampUnit})`;
  }

  public toMillis(timestamp: string) {
    if (this.dialect === "snowflake") {
      return `date_part(epoch_milliseconds, ${timestamp})`;
    }
    if (this.dialect === "postgres") {
      return `extract('epoch' from ${timestamp})::bigint * 1000`;
    }
    return `unix_millis(${timestamp})`;
  }

  public currentUTC() {
    if (this.dialect === "postgres") {
      return "current_timestamp::timestamp";
    }
    if (this.dialect === "snowflake") {
      return "convert_timezone('UTC', current_timestamp())::timestamp";
    }
    if (this.dialect === "mssql") {
      return "CURRENT_TIMESTAMP";
    }
    return "current_timestamp()";
  }
}
