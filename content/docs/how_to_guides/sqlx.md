---
title: SQLX
---

## Overview

SQLX is a powerful extension of SQL, created by Dataform. As it is an extension, every SQL file is also a valid SQLX file!

## Structure

SQLX contains the following components:

- [Config](#Config): contains information on the purpose of the script, such as `view` or `table`.

- [Pre-operations](#pre-operations): Pre operations contain SQL to be executed before the main bulk of SQL.

- (Central) [SQL](sql): The main SQL operation to be performed. Later defined Javascript can be injected here, or built in functions such as [`ref()`](TODO).

- [Post-operations](#post-operations): Post opertations contain SQL to be executed after the main bulk of SQL

- [Javascript](#javascript-blocks): These provide all the incredible functionality of Javascript written alongside SQL!

- [In-line Javascript](#in-line-javascript): In addition to Javascript blocks, in-line javascript can be written within SQL by using `${}`, for example `${console.log("foo")}`.

- [Built-in functions](built-in-functions): There are various useful built-in functions that can be used, such as `ref()` or `self()`.

### Config

All config properties, and the config itself, are optional. TODO: Add more detail. Just link API reference?

### Pre-operations

Pre-operation blocks are defined in SQLX by writing `pre-operations { }`.

SQL written in pre-operation blocks is executed before the central chunk of SQL.

TODO: Add more detail, explain why they're useful.

### SQL

Anything written outside of control blocks (`{}`) is interpreted as SQL. Therefore to start an SQL block, just close off any prior blocks.

### Post-operations

Post-operation blocks are defined in SQLX by writing `post-operations { }`.

SQL written in post-operation blocks is executed after the central chunk of SQL.

TODO: Add more detail, explain why they're useful.

### Javascript blocks

Javascript blocks are defined in SQLX by writing `js { }`.

Javascript blocks in SQLX provide all the incredible functionality of Javascript written alongside SQL!

For more examples see (TODO: add links to examples or how tos that use javascript.)

### In-line javascript

In-line javascript can be used anywhere SQL is written in order to dynamically modify the query.

For example,

```SQL
SELECT * FROM ${example}

js {
  const example = "foo";
}
```
