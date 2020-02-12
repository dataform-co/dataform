---
title: Assertions
priority: 2
---

## Introduction

Assertions enable you to check the state of data produced by other actions.

An assertion query is written to find rows that violate one or more rules. If the query returns any rows, then the assertion will fail.

![Assertions](/static/images/assertions.png)

## Example: Null checks

Modern warehouses often don't have a way to strongly enforce non-null fields.
To assert that fields `a`, `b`, and `c` are never `NULL` in a dataset named `sometable`, create a file `definitions/assert_sometable_not_null.sqlx`:

```js
config { type: "assertion" }

SELECT * FROM ${ref("sometable")}
WHERE a IS NOT NULL
  AND b IS NOT NULL
  AND c IS NOT NULL
```

If `a`, `b`, or `c` are null for any row in `sometable`, then this assertion will fail.

## Example: Key uniqueness checks

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
config {
  type: "view",
  dependencies: [ "table1_not_null" ]
}

SELECT * FROM ${ref("table1")} LEFT JOIN ...
```
