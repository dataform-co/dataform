---
title: Run unit tests on your queries
subtitle: Learn how to run unit tests on your queries.
priority: 8
---

## Introduction

Unit tests give you confidence that your code produces the output data you expect. Unit tests differ from assertions in that assertions are used to check the contents of datasets in your data warehouse,
while unit tests are used to validate your SQL code. Assertions verify _data_, and unit tests verify _logic_.

A SQLX unit test passes fake input to a `table` or `view` query, checking that the output rows match some expected output data.

## Example

Suppose we have the following `view` SQLX query:

```js
config {
  type: "view",
  name: "age_groups"
}
SELECT
  FLOOR(age / 5) * 5 AS age_group,
  COUNT(1) AS user_count
FROM ${ref("ages")}
GROUP BY age_group
```

We might want to write a unit test to check that the `age_groups` query works as we expect it to. Create a file `definitions/test_age_groups.sqlx`:

```js
config {
  type: "test",
  dataset: "age_groups"
}

input "ages" {
  SELECT 15 AS age UNION ALL
  SELECT 21 AS age UNION ALL
  SELECT 24 AS age UNION ALL
  SELECT 34 AS age
}

SELECT 15 AS age_group, 1 AS user_count UNION ALL
SELECT 20 AS age_group, 2 AS user_count UNION ALL
SELECT 30 AS age_group, 1 AS user_count
```

This unit test replaces the `ages` input to the `age_groups` query, and checks that the resulting output rows match the three expected `age_group` rows.

## Expected vs. actual output equality rules

A unit test fails if the actual output from the dataset is not equal to the expected output. This means that:

- the number of output rows must match
- the number of output columns and their names must match
- the contents of each row must match

Note that unit tests do not fail if columns are not output in the same order.

## Test hermiticity

As with unit testing in other languages and frameworks, it's considered bad practice for a unit test to be non-hermetic. This means that running your test
should have no dependencies on any state in your data warehouse. Thus, for queries you'd like to test, all input datasets should be referenced using `ref()` or `resolve()`. The test then injects the fake inputs
provided for each input dataset.

## Running unit tests

If you use the [Dataform CLI](../dataform-cli), you can run all tests in your project directory with the `dataform test` command.
