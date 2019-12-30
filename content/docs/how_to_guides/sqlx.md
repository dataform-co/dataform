---
title: SQLX
priority: 0
---

## Overview

SQLX is a powerful extension of SQL. As it is an extension, **every SQL file is also a valid SQLX file**!

## Structure

SQLX contains the following components:

- [Config](#config): contains information on the purpose of the script, such as `view` or `table`.

- [SQL](#sql): The SQL operation(s) to be performed. [In-line Javascript](#in-line-javascript) or [built-in functions](#built-in-functions) can be injected here.

- [Javascript](#javascript-blocks): These provide all the incredible functionality of Javascript written alongside SQL!

- [In-line Javascript](#in-line-javascript): In addition to Javascript blocks, in-line Javascript can be written within SQL by using `${}`, for example `${console.log("foo")}`.

- [Built-in functions](#built-in-functions): There are various useful built-in functions that can be used, such as `ref()` or `self()`.

### Config

All config properties, and the config itself, are optional. See the API reference for exact options.

### SQL

Anything written outside of control blocks (`{}`) is interpreted as SQL. Therefore to start an SQL block, just close off any prior blocks.

### Javascript blocks

Javascript blocks are defined in SQLX by writing `js { }`.

Javascript blocks in SQLX provide all the incredible functionality of Javascript written alongside SQL!

### In-line Javascript

In-line Javascript can be used anywhere SQL is written in order to dynamically modify the query.

For example,

```SQL
SELECT * FROM ${example}

js {
  const example = "foo";
}
```

### Built-in functions

Built in functions have special functionality should be executed within [in-line Javascript](#in-line-javascript).

#### `ref()`

`ref()` enables you to easily reference another dataset in your project without having to provide the full SQL dataset name. `ref()` also adds the referenced dataset to the set of dependencies for the query.

An example of `ref()` being used to add a dependency is [here](datasets/#referencing-other-datasets).

#### `resolve()`

`resolve()` works similarly to `ref()`, but doesn't add the dataset to the dependency list for the query.

#### `self()`

`self()` returns the name of the current dataset. If the default schema or dataset name is overridden in the `config{}` block, `self()` will return the full and correct dataset name.

An example of `self()` being used to set up incremental tables is [here](incremental-datasets/#a-simple-example).

## Additional Features

- [Pre-operations and post-operations](#pre-operations-and-post-operations): Pre and post operations are only valid for some table types.

### Pre-operations and post-operations

Pre-operation and post-operation blocks are defined in SQLX by writing `pre_operations { }` and `post_operations { }` respectively.

SQL written in pre-operation blocks is executed before the central chunk of SQL, while post-operation blocks are executed afterwards.

They are useful for purposes such as setting permissions. An example can be found in the [publishing datasets guide](datasets).
