---
title: SQLX
---

## Overview

SQLX is a powerful extension of SQL, written by the Dataform team. As it is an extension, every SQL file is also a valid SQLX file!

## Structure

We recommend writing SQLX using the follow structure.

- [Config](config): contains information on the purpose of the script, and what actions it is meant to take, such as `view` or `table`.

- [Pre-operations](pre_operations): Pre operations contain SQL to be executed before the main bulk of SQL.

- [SQL](sql): The central SQL operation to be performed. Later defined avascript can be injected here, or built in functions such as [`ref()`](TODO).

- [Post-operations](post_operations): Post opertations contain SQL to be executed after the main bulk of SQL

- [Javascript](javascript_blocks):

In addition to these blocks, in-line javascript within SQL can be written by using `${}`, for examples `${console.log("foo")}`.

### Config

All config properties, and the config itself, are optional.

```javascript
config {
```

<!--
export interface TConfig {
  type?: TableType;
  dependencies?: Resolvable | Resolvable[];
  tags?: string[];
  description?: string;
  columns?: IColumnsDescriptor;
  disabled?: boolean;
  protected?: boolean;
  redshift?: dataform.IRedshiftOptions;
  bigquery?: dataform.IBigQueryOptions;
  sqldatawarehouse?: dataform.ISQLDataWarehouseOptions;
  database?: string;
  schema?: string;
} -->

> PARAMETER: `"type":` (string) - Action for the SQLX file to perform. Must take the form of one of:
>
> > VALUE: `"table"` - Create a table.
> >
> > `"view"` - Retrieve/transform an existing table.
> >
> > `"incremental"` - Create an incremental table.
> >
> > `"inline"` - TODO.
>
> `"dependencies:"` - A "dependency", or ["list", "of", "dependencies"] that this SQLX file depends on.
>
> `"tags:"` -

... TODO

```javascript
}
```

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
  const example =
}
```
