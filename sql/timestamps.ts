import { ISqlDialect } from "df/sql";

export type DurationUnit =
  | "millisecond"
  | "second"
  | "minute"
  | "hour"
  | "day"
  | "week"
  | "month"
  | "quarter"
  | "year";

export class Timestamps {
  constructor(private readonly dialect: ISqlDialect) {}

  public fromMillis(timestampMillis: string) {
    if (this.dialect === "snowflake") {
      return `to_timestamp(${timestampMillis}, 3)`;
    }
    if (this.dialect === "postgres" || this.dialect === "redshift") {
      return `timestamp 'epoch' + (${timestampMillis}) * interval '0.001 second'`;
    }
    return `timestamp_millis(${timestampMillis.toString()})`;
  }

  public truncate(timestamp: string, timestampUnit: DurationUnit) {
    if (this.dialect === "snowflake") {
      return `date_trunc(${timestampUnit}, ${timestamp})`;
    }
    if (this.dialect === "postgres" || this.dialect === "redshift") {
      return `date_trunc('${timestampUnit}', ${timestamp})`;
    }
    return `timestamp_trunc(${timestamp}, ${timestampUnit})`;
  }

  public toMillis(timestamp: string) {
    if (this.dialect === "snowflake") {
      return `date_part(epoch_milliseconds, ${timestamp})`;
    }
    if (this.dialect === "postgres" || this.dialect === "redshift") {
      return `extract('epoch' from ${timestamp})::bigint * 1000`;
    }
    return `unix_millis(${timestamp})`;
  }

  public currentUTC() {
    if (this.dialect === "postgres" || this.dialect === "redshift") {
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

  public diff(datePart: DurationUnit, start: string, end: string) {
    if (this.dialect === "standard") {
      return `timestamp_diff(${end}, ${start}, ${datePart})`;
    }
    if (this.dialect === "snowflake" || this.dialect === "mssql" || this.dialect === "redshift") {
      return `datediff(${datePart}, ${start}, ${end})`;
    }
    if (this.dialect === "postgres") {
      if (datePart.toLowerCase() === "day") {
        return `date_part('day', ${end} - ${start})`;
      }
      if (datePart.toLowerCase() === "hour") {
        return `24 * date_part('day', ${end} - ${start}) + date_part('hour', ${end} - ${start})`;
      }
      if (datePart.toLowerCase() === "minute") {
        return `24 * date_part('day', ${end} - ${start}) + 60 * date_part('hour', ${end} - ${start}) + date_part('minute', ${end} - ${start})`;
      }
      if (datePart.toLowerCase() === "second") {
        return `24 * date_part('day', ${end} - ${start}) + 60 * date_part('hour', ${end} - ${start}) + 60 * date_part('minute', ${end} - ${start}) + date_part('second', ${end} - ${start})`;
      }
      if (datePart.toLowerCase() === "millisecond") {
        return `24 * date_part('day', ${end} - ${start}) + 60 * date_part('hour', ${end} - ${start}) + 60 * date_part('minute', ${end} - ${start}) + 1000 * date_part('second', ${end} - ${start}) + date_part('millisecond', ${end} - ${start})`;
      }
    }
  }

  public add(timestamp: string, units: number, datePart: DurationUnit) {
    if (this.dialect === "standard") {
      return `timestamp_add(${timestamp}, interval ${units} ${datePart})`;
    }
    if (this.dialect === "postgres") {
      return `${timestamp} + interval '1 ${datePart}' * ${units}`;
    }
    if (this.dialect === "snowflake") {
      return `timestampadd(${datePart}, ${units}, ${timestamp})`;
    }
    return `dateadd(${datePart}, ${units}, ${timestamp})`;
  }
}
