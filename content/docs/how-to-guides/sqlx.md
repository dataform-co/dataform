---
title: SQLX
---

## Overview

SQLX is a powerful extension of SQL. As it is an extension, **every SQL file is also a valid SQLX file**!

## Structure

SQLX contains the following components:

- [Config](#config): contains information on the purpose of the script, such as `view` or `table`.

- [SQL](#sql): The SQL operation(s) to be performed. [In-line Javascript](#in-line-javascript) or [built-in functions](#built-in-functions) can be injected here.

- [Javascript](#javascript) (and in-line Javascript): These provide all the incredible functionality of Javascript written alongside SQL!

- [Built-in functions](#built-in-functions): There are various useful built-in functions that can be used, such as `ref()` or `self()`.

### Config

All config properties, and the config itself, are optional. See [`ITableConfig` in the API reference](/reference#ITableConfig) for exact options.

### SQL

Anything written outside of control blocks (`{}`) is interpreted as SQL. Therefore to start an SQL block, just close off any prior blocks.

### Javascript

Javascript can be used within SQLX via a Javascript block, which can then be injected into the SQL using in-line Javascript in order to dynamically modify the query.

For example:

```SQL
js {
  const example = "foo";
}

SELECT * FROM ${example}
```

#### Javascript Blocks

Javascript blocks are defined in SQLX by writing `js { }`.

JavaScript blocks in SQLX can be used for defining reusable functions that can be used to generate repetitive parts of SQL code.

#### In-line Javascript

In-line Javascript can be used anywhere SQL is written in order to dynamically modify the query. It is injected by using `${}`, for example `${console.log("foo")}`.

### Built-in functions

Built in functions have special functionality and can be executed either within [in-line Javascript](#in-line-javascript) or [javascript blocks](#javascript-blocks).

For all built in functions, see [`ITableContext` in the API reference](/reference#ITableContext). Some useful examples can be found here:

- [Reference another dataset in your project with `ref()`](datasets/#referencing-other-datasets)

- [Retrieve the name of the current dataset with `self()`](incremental-datasets/#a-simple-example)

- [Execute code only if the script is for an incremental dataset using `ifIncremental()`](incremental-datasets/#conditional-code-if-incremental)

## Additional Features

- **Post-operations**: defined in SQLX by writing `post_operations { }`, SQL writtin inside will be executed after the central SQL. This can be useful for granting permissions, as can be seen in the [publishing datasets guide](/how-to-guides/datasets/#example-granting-dataset-access-with-post_operations). **Actions may only include pre_operations if they create a dataset**, for example with `type: "table"` or `type: "view"` or `type: "incremental"` in their config.

- **Pre-operations**: the same as post-operations, but defined with `pre_operations { }`, and takes place before the central SQL.

- **Incremental where**: defined in SQLX by writing `incremental_where { }`. Only valid for incremental tables (those with `type: "incremental"` in the config). This wraps the main block of SQL in a `WHERE` clause, with the argument of whatever's inside the block. For examples see the [incremental datasets how to guide](http://localhost:3001/how-to-guides/incremental-datasets).
