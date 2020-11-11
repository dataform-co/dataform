---
title: SQLX
subtitle: Learn about the structure and features of SQLX files.
priority: 1
---

## Overview

SQLX is a powerful extension of SQL. As it is an extension, **every SQL file is also a valid SQLX file**!

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" >
<h4 class="bp3-heading">New to SQLX and Dataform?</h4>
<p>
If you are new to SQLX and Dataform, reading the introduction could be helpful to understand SQLX, its features, and how it works.
</p>
<a href="/introduction/dataform-in-5-minutes"><button intent="primary">How SQLX works in 5 min</button></a></div>

## Structure

SQLX contains the following components:

- [Config](#config): contains information on the purpose of the script, such as `view` or `table`.

- [SQL](#sql): The SQL operation(s) to be performed. [In-line JavaScript](#in-line-javascript) or [built-in functions](#built-in-functions) can be injected here.

- [JavaScript](#javascript) (and in-line JavaScript): These provide all the incredible functionality of JavaScript written alongside SQL!

- [Built-in functions](#built-in-functions): There are various useful built-in functions that can be used, such as `ref()` or `self()`.

### Config

All config properties, and the config itself, are optional. See [`ITableConfig` in the API reference](/reference#ITableConfig) for exact options.

### SQL

Anything written outside of control blocks (`{}`) is interpreted as SQL. Therefore to start an SQL block, just close off any prior blocks.

### JavaScript

JavaScript can be used within SQLX via a JavaScript block, which can then be injected into the SQL using in-line JavaScript in order to dynamically modify the query.

For example:

```SQL
js {
  const example = "foo";
}

SELECT * FROM ${example}
```

#### JavaScript Blocks

JavaScript blocks are defined in SQLX by writing `js { }`.

JavaScript blocks in SQLX can be used for defining reusable functions that can be used to generate repetitive parts of SQL code.

#### In-line JavaScript

In-line JavaScript can be used anywhere SQL is written in order to dynamically modify the query. It is injected by using `${}`, for example `${console.log("foo")}`.

### Built-in functions

Built in functions have special functionality and can be executed either within [in-line JavaScript](#in-line-javascript) or [JavaScript blocks](#javascript-blocks).

For all built in functions, see [`ITableContext` in the API reference](/reference#ITableContext). Some useful examples can be found here:

#### ref()

`ref()` enables you to easily reference another dataset in your project without having to provide the full SQL dataset name. `ref()` also adds the referenced dataset to the set of dependencies for the query.

Some examples can be found [here](datasets#referencing-other-datasets).

#### resolve()

`resolve()` works similarly to `ref()`, but doesn't add the dataset to the dependency list for the query.

#### self()

`self()` returns the fully qualified name (database, schema and name) of the current dataset. If the database, schema, or dataset name is overridden in the `config{}` block, `self()` will return the full and correct dataset name.

#### name()

`name()` returns the name of the current dataset.

[Here](incremental-datasets/#a-simple-example) is an example of an incremental table using the `self()` function.

- [Retrieve the name of the current dataset with `self()`](incremental-datasets#a-simple-example).

- [Execute code only if the script is for an incremental dataset using `incremental()`](incremental-datasets#conditional-code-if-incremental).

## Additional Features

- **Pre-operations**: defined in SQLX by writing `pre_operations { }`, SQL written inside will be executed before the main SQL. This can be useful for granting permissions, as can be seen in the [publishing datasets guide](datasets#example-granting-dataset-access-with-post_operations). **Actions may only include pre_operations if they create a dataset**, for example with `type: "table"` or `type: "view"` or `type: "incremental"` in their config.

- **Post-operations**: the same as pre-operations, but defined with `post_operations { }`, and runs after the main SQL.

BigQuery and SQL Data Warehouse run all operations for a file (such as pre and post operations) in the same context; the executed SQL is created by joining all operations with a semi-colon `;`. For other warehouse types, operations are run as separate queries.

This is useful for scripting, for example defining variables or UDFs in BigQuery before the create table statement.

## Sandboxing for fast, secure, reproducible SQLX compilation

Dataform executes all JavaScript and SQLX code in a Dataform project inside a sandboxed environment that has no access to the database, data, or network.

While this can be limiting in some cases, there are many benefits to this approach.

#### Speed

Dataform compilation is extremely fast, and can compile projects with hundreds of datasets and tens of thousands of lines of code in under a second. This is useful for development where you can immediately see what your code will do without waiting.

#### Reproducibility

Changing your generated SQL queries based on data in the warehouse can lead to pipelines breaking even when you haven't changed code. This can be frustrating to debug, and means table structures can change for downstream consumers without notice. With Dataform and SQLX, your build steps and generated table schematas are generally fixed, making debugging and reproducibility much easier.

#### Security

Third party code (such as dataform packages) shouldn't be able to read any of your data. As all project code runs in the sandbox, there is no way to read and leak your data, instead the worst a malicious package could do is cause SQL queries to execute in your warehouse.
