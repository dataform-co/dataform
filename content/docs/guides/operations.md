---
title: Custom SQL operations
priority: 3
---

## Introduction

An `operations` file defines a set of SQL commands that will be executed in order against your warehouse. Operations can be used to run custom SQL that doesn't necessarily fit into the model of publishing a dataset or writing an assertion.

## Simple example

To define a new `operations` action, create a file such as `definitions/example_operation.sqlx`:

```js
CREATE OR REPLACE VIEW someschema.someview AS (SELECT 1 AS test)
```

Multiple statements can be separated with a single line containing only 3 dashes `---`:

```js
CREATE OR REPLACE VIEW someschema.someview AS (SELECT 1 AS test)
---
DROP VIEW IF EXISTS someschema.someview
```

These statements will be run without modification against the warehouse. You can use warehouse specific commands in these files, such as BigQuery's DML or DDL statements, or Redshift/Postgres specific commands.

Operations files behave very similarly to the statements provided to the `pre_operations { ... }` and `post_operations { ... }` blocks used when publishing datasets.

## Custom dataset builds

In some cases, you may want to create a dataset manually rather than relying on Dataform's built-in logic. This can be achieved by writing an operations file and specifying the full `CREATE TABLE/VIEW` commands manually:

```js
CREATE OR REPLACE VIEW sometable.someschema AS (SELECT 1 AS TEST)
```

## Declaring and referencing outputs

If an operation creates a dataset or view that you would like to make available to other scripts, you can reference this operation as you would a normal dataset by using the `ref()` function. Note that operations may use `self()` to create a dataset or view that matches the current file name.

For example, in `definitions/customview.sqlx`:

```js
config { hasOutput: true }
CREATE OR REPLACE VIEW ${self()} AS (SELECT 1 AS TEST)
```

References to `"customview"` will now resolve to `"defaultschema.customview"` and can be used in other SQL files, for example:

```js
SELECT * FROM ${ref("customview")}
```

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  The output dataset created by the operation must match the name of the file in order for
  references to work properly. We recommend using <code>self()</code> to enforce this requirement.
  If you would like to create a dataset in a custom schema, or override the dataset's name, use the{" "}
  <code>schema</code> or <code>name</code> configuration settings.
</div>

## Example: Running `VACUUM` commands (in Postgres or Redshift)

Postgres and Redshift have a `VACUUM` command that can be used to improve the performance of some datasets. This is a common use case for operations:

```js
VACUUM DELETE ONLY ${ref("sometable")} TO 75 PERCENT
---
VACUUM REINDEX ${ref("sometable")}
```
