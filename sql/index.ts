import { AggregateBuilder } from "df/sql/builders/aggregate";
import { FromBuilder } from "df/sql/builders/from";
import { IJoins, JoinBuilder } from "df/sql/builders/join";
import { JSONBuilder } from "df/sql/builders/json";
import { ISelectOrBuilder, ISelectSchema } from "df/sql/builders/select";
import { UnionBuilder } from "df/sql/builders/union";
import { IWiths, WithBuilder } from "df/sql/builders/with";

export type DurationUnit = "day" | "week" | "month" | "quarter" | "year";

export interface IConditional {
  condition: string;
  then: string;
  else?: string;
}

export class Sql {
  public static create() {
    return new Sql();
  }

  public literal(value: string | number) {
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

  public conditional(conditional: IConditional) {
    return `if(${conditional.condition}, ${conditional.then}, ${conditional.else || "null"})`;
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

  public asTimestampFromMillis(timestampMillis: number) {
    return `timestamp_millis(${timestampMillis.toString()})`;
  }

  public asTimestamp(castableToTimestamp: string) {
    return `cast(${castableToTimestamp} as timestamp)`;
  }

  public asTruncatedTimestamp(castableToTimestamp: string, timestampUnit: DurationUnit) {
    return `timestamp_trunc(cast(${castableToTimestamp} as timestamp), ${timestampUnit})`;
  }

  public asTruncatedUnixMillis(castableToTimestamp: string, timestampUnit: DurationUnit) {
    return `unix_millis(${this.asTruncatedTimestamp(castableToTimestamp, timestampUnit)})`;
  }

  public withWrappingBrackets(expression: string) {
    return `(${expression})`;
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
