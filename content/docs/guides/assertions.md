---
title: Test data quality with assertions
subtitle: Learn how to test data quality with assertions
priority: 3
---

## Introduction

Assertions enable you to check the state of data produced by other actions.

An assertion query is written to find rows that violate one or more rules. If the query returns any rows, then the assertion will fail.

![Assertions](/static/images/assertions.png)

## Auto-generated assertions

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  Auto generated assertions are only supported from <code>@dataform/core</code> version <code>1.4.13</code>.
</div>

Dataform provides a convenient way to define assertions as part of a dataset's `config` settings.

Here's a complete example:

```js
// definitions/my_table.sqlx
config {
  type: "table",
  assertions: {
    uniqueKey: ["user_id"],
    nonNull: ["user_id", "customer_id"],
    rowConditions: [
      'signup_date is null or signup_date > "2019-01-01"',
      'email like "%@%.%"'
    ]
  }
}
select ...
```

For all configuration options, view the [reference documentation for dataset assertions](/reference#ITableAssertions).

## Unique keys

If the `uniqueKey` property is set, the resulting assertion will fail if there is more than one row in the dataset with the same values for all of the column(s).

```js
config {
  type: "table",
  assertions: {
    uniqueKey: ["user_id"]
  }
}
select ...
```

Multiple key columns can be provided.

The generated assertion will be called `<original_dataset_name>_assertions_uniqueKey`.

## Non-nullness

To assert that a set of columns are never null, provide an array of the column names with the `nonNull` property:

```js
config {
  type: "table",
  assertions: {
    nonNull: ["user_id", "customer_id", "email"]
  }
}
select ...
```

The generated assertion will be called `<original_dataset_name>_assertions_rowConditions`, because `nonNull` is just shorthand for custom row conditions (see below).

## Custom row conditions

For assertions that require custom logic to be evaluated against rows, use the `rowConditions` property. Each row condition should be a SQL expression that is expected to evaluate to `true` if the assertion should pass.

```js
config {
  type: "table",
  assertions: {
    rowConditions: [
      'signup_date is null or signup_date > "2019-01-01"',
      'email like "%@%.%"'
    ]
  }
}
select ...
```

Each row will be evaluated against each condition, and all rows must pass all conditions for the assertion to pass.

The generated assertion will be called: `<original_dataset_name>_assertions_rowConditions`.

## Manual assertions

Assertions can also be defined manually for more advanced use cases, or for testing datasets that aren't created by Dataform.

To write a manual assertion, create a new SQLX file and set the type to `assertion` in the `config` block:

```js
// definitions/custom_assertion.sqlx
config {
  type: "assertion"
}
```

To write the assertion, write a new SQL query below the config block that should return zero rows. The best way to think about writing this queries is that they are **queries that look for errors**.

For example, to assert that fields `a`, `b`, and `c` are never `NULL` in a dataset named `sometable`, create a file `definitions/assert_sometable_not_null.sqlx`:

```js
// definitions/assert_sometable_not_null.sqlx
config { type: "assertion" }

SELECT
  *
FROM
  ${ref("sometable")}
WHERE
  a IS NULL
  OR b IS NULL
  OR c IS NULL
```

Another common requirement is to check that all values for a particular field or combination of fields are unique in a dataset.
For example, in a `daily_customer_stats` dataset, there should only ever be a single row for each combination of the `date` and `customer_id` fields.

You can assert these requirements as follows:

```js
config { type: "assertion" }

WITH base AS (
SELECT
  date,
  customer_id,
  SUM(1) as rows
FROM ${ref("daily_customer_stats")}
)
SELECT * FROM base WHERE rows > 1
```

This query will find any keys where there exists more than 1 row for that key (and thus are not unique).

## Inspecting failed assertion rows

Dataform automatically creates a view in your warehouse containing the results of the compiled assertion query.
This makes it easy to inspect the rows that caused the assertion to fail without increasing storage requirements or pulling any data out of your warehouse.

Assertions create views in a seperate schema to your default schema. This is configured in your [`dataform.json`](configuration) file.

Given a default assertion schema `dataform_assertions` and an assertion file called `definitions/some_assertion.sqlx`:

```js
config { type: "assertion" }
SELECT * FROM ${ref("example")} WHERE test > 1
```

Dataform will create a view called `dataform_assertions.some_assertion` in your warehouse using the following query:

```js
CREATE OR REPLACE VIEW dataform_assertions.some_assertion AS SELECT * from dataform.example WHERE test > 1
```

You can manually inspect this view to debug failing assertions.

## Assertions as dependencies

Assertions create named actions in your project that can be depended upon by using the `dependencies` configuration parameter.

If you would like another dataset, assertion, or operation to only run if a specific assertion passes, you can add the assertion to that action's dependencies.

For example, given two datasets called `table1` and `table2`, and an assertion called `table1_not_null`, if you want to ensure that `table2` only
runs if `table1_not_null` passes, you could add it as a dependency in `definitions/table2.sqlx`:

```js
// definitions/table2.sqlx
config {
  type: "view",
  dependencies: [ "table1_not_null" ]
}

SELECT * FROM ${ref("table1")} LEFT JOIN ...
```
