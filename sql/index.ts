import { AggregateBuilder } from "df/sql/builders/aggregate";
import { FromBuilder } from "df/sql/builders/from";
import { IJoins, JoinBuilder } from "df/sql/builders/join";
import { JSONBuilder } from "df/sql/builders/json";
import { ISelectOrBuilder, ISelectSchema } from "df/sql/builders/select";
import { UnionBuilder } from "df/sql/builders/union";
import { IWiths, WithBuilder } from "df/sql/builders/with";
import { Timestamps } from "df/sql/timestamps";

export type ISqlDialect = "standard" | "snowflake" | "postgres" | "mssql" | "redshift";

export class Sql {
  public readonly timestamps: Timestamps;

  constructor(private readonly dialect: ISqlDialect = "standard") {
    this.timestamps = new Timestamps(dialect);
  }

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
    if (this.dialect === "postgres" || this.dialect === "redshift") {
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

  // Casting functions.

  public asTimestamp(castableToTimestamp: string) {
    return `cast(${castableToTimestamp} as timestamp)`;
  }

  public asString(castableToString: string) {
    if (this.dialect === "postgres" || this.dialect === "redshift") {
      return `cast(${castableToString} as varchar)`;
    }
    return `cast(${castableToString} as string)`;
  }

  // Surrogate keys.

  public surrogateKey(columnNames: string[]) {
    const columnsAsStrings = columnNames.map(id => this.asString(id)).join(`,`);
    if (this.dialect === "standard") {
      return this.asString(`farm_fingerprint(concat(${columnsAsStrings}))`);
    }
    if (this.dialect === "mssql") {
      return this.asString(`hashbytes("md5", (concat(${columnsAsStrings})))`);
    }
    return this.asString(`md5(concat(${columnsAsStrings}))`);
  }

  // window function

  public windowFunction(
    name: string,
    value: string,
    ignoreNulls: boolean = false,
    windowSpecification?: {
      partitionFields?: string[];
      orderFields?: string[];
      frameClause?: string;
    }
  ) {
    const partitionFieldsAsString = windowSpecification.partitionFields
      ? [...windowSpecification.partitionFields].join(`, `)
      : "";
    const orderFieldsAsString = windowSpecification.orderFields
      ? [...windowSpecification.orderFields].join(`, `)
      : "";

    if (this.dialect === "standard" || this.dialect === "mssql" || this.dialect === "snowflake") {
      return `${name}(${value} ${ignoreNulls ? `ignore nulls` : ``}) over (${
        windowSpecification.partitionFields ? `partition by ${partitionFieldsAsString}` : ``
      } ${windowSpecification.orderFields ? `order by ${orderFieldsAsString}` : ``} ${
        windowSpecification.frameClause ? windowSpecification.frameClause : ``
      })`;
    }

    // For some window functions in Redshift, a frame clause is always required
    const requiresFrame = [
      "avg",
      "count",
      "first_value",
      "last_value",
      "max",
      "min",
      "nth_value",
      "stddev_samp",
      "stddev_pop",
      "stddev",
      "sum",
      "variance",
      "var_samp",
      "var_pop"
    ].includes(name.toLowerCase());

    if (this.dialect === "redshift") {
      return `${name}(${value} ${ignoreNulls ? `ignore nulls` : ``}) over (${
        windowSpecification.partitionFields ? `partition by ${partitionFieldsAsString}` : ``
      } ${windowSpecification.orderFields ? `order by ${orderFieldsAsString}` : ``} ${
        windowSpecification.orderFields
          ? windowSpecification.frameClause
            ? windowSpecification.frameClause
            : requiresFrame
            ? `rows between unbounded preceding and unbounded following`
            : ``
          : ``
      })`;
    }

    if (this.dialect === "postgres") {
      return `${name}(${value}) over (${
        windowSpecification.partitionFields ? `partition by ${partitionFieldsAsString}` : ``
      } ${windowSpecification.orderFields || ignoreNulls ? `order by` : ``} ${
        ignoreNulls ? `case when ${value} is not null then 0 else 1 end asc` : ``
      } ${orderFieldsAsString && ignoreNulls ? `,` : ``} ${orderFieldsAsString} ${
        windowSpecification.orderFields
          ? windowSpecification.frameClause
            ? windowSpecification.frameClause
            : requiresFrame
            ? `rows between unbounded preceding and unbounded following`
            : ``
          : ``
      })`;
    }
  }

  // String aggregation

  public stringAgg(field: string, delimiter = ",") {
    if (this.dialect === "snowflake" || this.dialect === "redshift") {
      return `listagg(${field}, '${delimiter}')`;
    }
    return `string_agg(${field}, '${delimiter}')`;
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
