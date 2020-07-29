import { AggregateBuilder } from "df/sql/builders/aggregate";
import { FromBuilder } from "df/sql/builders/from";
import { IJoins, JoinBuilder } from "df/sql/builders/join";
import { JSONBuilder } from "df/sql/builders/json";
import { ISelectOrBuilder, ISelectSchema } from "df/sql/builders/select";
import { UnionBuilder } from "df/sql/builders/union";
import { IWiths, WithBuilder } from "df/sql/builders/with";

export type DurationUnit = "day" | "week" | "month" | "quarter" | "year";

export type ISqlDialect = "standard" | "snowflake" | "postgres" | "mssql";

export class Sql {
  constructor(private readonly dialect: ISqlDialect = "standard") {}

  public literal(value: string | number | null) {
    if (value === null) {
      return "null";
    }
    if (typeof value === "string") {
      return `'${value}'`;
    }
    return String(value);
  }

  public not(expression: string): string {
    return `not (${expression})`;
  }

  public countDistinct(expression: string) {
    return `count(distinct ${expression})`;
  }

  public conditional(condition: string, then: string, otherwise: string = "null") {
    if (this.dialect === "snowflake") {
      return `iff(${condition}, ${then}, ${otherwise || "null"})`;
    }
    if (this.dialect === "postgres") {
      return `case when ${condition} then ${then} else ${otherwise} end`;
    }
    return `if(${condition}, ${then}, ${otherwise || "null"})`;
  }

  public equals(expression: string, expected: string) {
    if (expected.trim().toLowerCase() === "null") {
      return `${expression} is null`;
    }
    return `${expression} = ${expected}`;
  }

  public gt(left: string, right: string) {
    return `${left} > ${right}`;
  }

  public gteq(left: string, right: string) {
    return `${left} >= ${right}`;
  }

  public lt(left: string, right: string) {
    return `${left} < ${right}`;
  }

  public lteq(left: string, right: string) {
    return `${left} <= ${right}`;
  }

  public in(expression: string, values: string[]) {
    if (values.length === 0) {
      // If there are no values, then this must be false.
      return "false";
    }
    // You can't check for nulls using "in". Instead, create an or expression using is null.
    const containsNull = values.find(value => value.toLowerCase() === "null");
    const nonNullValues = values.filter(value => value.toLowerCase() !== "null");
    const ors: string[] = [];
    if (containsNull) {
      ors.push(this.equals(expression, "null"));
    }
    if (nonNullValues.length > 0) {
      ors.push(`${expression} in (${values.join(", ")})`);
    }
    return this.or(ors);
  }

  public sum(expression: string | number) {
    return `sum(${expression})`;
  }

  public min(expression: string | number) {
    return `min(${expression})`;
  }

  public max(expression: string | number) {
    return `max(${expression})`;
  }

  public avg(expression: string | number) {
    return `avg(${expression})`;
  }

  public count() {
    return `sum(1)`;
  }

  public coalesce(...expressions: string[]) {
    return `coalesce(${expressions.join(", ")})`;
  }

  public or(expressions: string[]) {
    return `(${expressions.join(" or ")})`;
  }

  public and(expressions: string[]) {
    return expressions.join(" and ");
  }

  public withWrappingBrackets(expression: string) {
    return `(${expression})`;
  }

  public safeDivide(numerator: string, denominator: string) {
    return `${numerator} / nullif(${denominator}, 0)`;
  }

  // Conversion functions.

  public millisToTimestamp(timestampMillis: string) {
    if (this.dialect === "snowflake") {
      return `to_timestamp(${timestampMillis}, 3)`;
    }
    if (this.dialect === "postgres") {
      return `timestamp 'epoch' + (${timestampMillis} / 1000) * interval '1 second'`;
    }
    return `timestamp_millis(${timestampMillis.toString()})`;
  }

  public timestampTruncate(timestamp: string, timestampUnit: DurationUnit) {
    if (this.dialect === "snowflake") {
      return `date_trunc(${timestampUnit}, ${timestamp})`;
    }
    if (this.dialect === "postgres") {
      return `date_trunc('${timestampUnit}', ${timestamp})`;
    }
    return `timestamp_trunc(${timestamp}, ${timestampUnit})`;
  }

  public timestampToMillis(timestamp: string) {
    if (this.dialect === "snowflake") {
      return `date_part(epoch_milliseconds, ${timestamp})`;
    }
    if (this.dialect === "postgres") {
      return `extract('epoch' from ${timestamp})::bigint * 1000`;
    }
    return `unix_millis(${timestamp})`;
  }

  public timestampCurrentUTC() {
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

  // Casting functions.

  public asTimestamp(castableToTimestamp: string) {
    return `cast(${castableToTimestamp} as timestamp)`;
  }

  public asString(castableToString: string) {
    if (this.dialect === "postgres") {
      return `cast(${castableToString} as varchar)`;
    }
    return `cast(${castableToString} as string)`;
  }

  // Convenience methods for builders.
  public json<S extends ISelectSchema>(data: S[]) {
    return new JSONBuilder<S>(this, data);
  }

  public join<J extends IJoins>(joins: J) {
    return new JoinBuilder(joins);
  }

  public with(withs: IWiths) {
    return new WithBuilder(withs);
  }

  public union<S extends ISelectSchema>(...selects: Array<ISelectOrBuilder<S>>) {
    return new UnionBuilder(selects);
  }

  public aggregate(from: ISelectOrBuilder<any>) {
    return new AggregateBuilder(from);
  }

  public from(from: ISelectOrBuilder<any>) {
    return new FromBuilder(from);
  }
}
