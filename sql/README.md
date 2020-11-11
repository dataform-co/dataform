Useful functions for constructing SQL statements in Dataform packages.

## Supported warehouses

- BigQuery
- Redshift
- Snowflake

_If you would like us to add support for another warehouse, please get in touch via [email](mailto:team@dataform.co) or [Slack](https://dataform.co/slack)_

## Installation

(TODO)

## Functions

### Common SQL functions

#### surrogateKey

Creates a unique hash from a list of fields. Useful for generating a surrogate key for a table.

`${sql.surrogateKey(["field_one", "field_two"])}`

#### windowFunction

Creates a window function with the given configuration.

`${sql.windowFunction({ name: "window_function_name", value: "target_column", ignoreNulls: true, windowSpecification: { partitionFields: ["field_to_partition_by_one", "field_to_partition_by_two"], orderFields: ["field_to_order_by_one", "field_to_order_by_two"], frameClause: "rows between 0 preceding and unbounded following" } })}`

#### asTimestamp

Casts the field to timestamp type

`${sql.asTimestamp("field")}`

#### asString

Casts the field to timestamp type

`${sql.asString("field")}`

#### stringAgg

Groups the values in a field into a concatenated string, with an optional delimiter value that defaults to ","

`${sql.stringAgg("field", "delimiter")}`

### Timestamp functions

#### diff

Calculate the time difference between two timestamps.

`${sql.timestamps.diff("date_part", "start_timestamp", "end_timestamp")}`

#### add

Add a period of time to a timestamp.

`${sql.timestamps.add("timestamp", 1, "hour")`
