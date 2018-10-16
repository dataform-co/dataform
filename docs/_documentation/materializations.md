---
layout: documentation
title: Materializations
sub_headers: ["Tables", "Incremental tables"]
---

# Materializations

A materialization defines a table, or view that will be created in your data warehouse.

To define a new materialization, create a `.sql` file in the `models` directory. The name of the file will be the name of the table created in your data warehouse.

```sql
-- myfirstmodel.sql
select 1 as test
```

Will create a `view` called `myfirstmodel.sql` in the default dataform schema defined in the [`dataform.json`](/docs/configuration/#dataform.json) file.

There are several configuration options that can be applied to a materialization. These can be applied by calling the appropriate method within a `${}` block, or can be provided all as one using the [options syntax](#Options syntax).

## Tables

By default, materializations are created as views in your warehouse. To create a copy of the query result as table, you can use the `type` method to change the materialization type to `"table"`.

```js
${type("table")}
---
select 1 as test
```

## Incremental tables

Incremental tables allow you build a table incrementally, by only inserting data that has not yet been processed.

In order to define an incremental table we must set the `type` of the query to `"incremental"`, and provide a where clause.

For example, if we have a timestamp field in our source table called `ts`, then we provide a where statement that means we only processes rows that are newer than the latest value `ts` in our output table. The `where` function should be used inline as part of a query.

```js
${type("incremental")}
---
select a, b from sourcetable
  ${where(`ts > (select max(ts) from ${self()}`)}
```

Incremental tables automatically produce the necessary `create table` and `insert` statements.

For the above example, if the table does not exist the the following statement will be run:

```sql
create or replace table schema.incrementalexample as
  select a, b
  from sourcetable
```

Subsequent runs will then run the following statement:

```sql
insert into schema.incrementalexample (a, b)
  select a, b
  from sourcetable
  where ts > (select max(ts) from schema.incrementaltable)
```

It's important to note that incremental tables MUST specifically list selected fields, so that the insert statement can be automatically generated.

## Options syntax

You can set several properties at once using the options syntax. The options method takes a JavaScript object as its argument:

```js
${options({
  type: "table",
  pre: "insert current_timestamp() into run_logs",
  dependencies: ["myothermodel"]
})}
---
select 1 as test
```
