---
layout: documentation
title: Materializations
sub_headers: ["Tables", "Incremental tables"]
---

A materialization defines a table, or view that will be created in your data warehouse.

To define a new materialization, create a `.sql` file in the `models` directory. The name of the file will be the name of the table created in your data warehouse.

For example, the file `models/example.sql` with the contents:

```js
select 1 as test
```

Will create a `view` called `example` in the default dataform schema defined in the [`dataform.json`](/configuration/#dataform.json) file.

There are many configuration options that can be applied to a materialization, for a full list see the [materializations reference](/reference/materializations).

## Tables

By default, materializations are created as views in your warehouse. To create a copy of the query result as table, you can use the `type` method to change the materialization type to `"table"`.

```sql
--js type("table");
select 1 as test
```

## Incremental tables

Incremental tables allow you build a table incrementally, by only inserting data that has not yet been processed.

In order to define an incremental table we must set the `type` of the query to `"incremental"`, and provide a where clause.

For example, if you have a timestamp field in a source table called `ts`, then you can provide a where statement that means only rows that are newer than the latest value value of `ts` in our output table. The `where` function should be used inline as part of a query.

<p>
<div class="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
When using incremental tables, you must describe all the columns in the table, so that the correct insert statements can be generated.
</div>
</p>

```sql
/*js
type("incremental");
where(`ts > (select max(ts) from ${self()}`);
descriptor("ts", "a", "b");
*/
select ts, a, b from sourcetable
```

Incremental tables automatically produce the necessary `create table` and `insert` statements.

For the above example, if the table does not exist the the following statement will be run:

```sql
create or replace table dataform.incrementalexample as
  select ts, a, b
  from sourcetable
```

Subsequent runs will then run the following statement:

```sql
insert into dataform.incrementalexample (ts, a, b)
  select ts, a, b
  from sourcetable
  where ts > (select max(ts) from dataform.incrementaltable)
```

It's important to note that incremental tables MUST specifically list selected fields, so that the insert statement can be automatically generated.

The following would not work:
```sql
--js type("incremental");
select * from sourcetable
```

## Pre operations

You can execute one or more statements before a table is materialized using the [`preOps()`](/built-in-functions#preOps) built-in:

```js
/*js
preOps([
  "run this before",
  "then run this before"
])
*/
select 1 as test
```

## Post operations

You can execute one or more statements after a table is materialized using the [`postOps()`](/built-in-functions#postOps) built-in:

```js
/*js
postOps([
  "run this after",
  "then run this after"
])
*/
select 1 as test
```
