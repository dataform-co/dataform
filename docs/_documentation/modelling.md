---
layout: documentation
title: Modelling
sub_headers: ["Materializations"]
---

Modelling
===

# Materializations

A materialization defines a table, or view that will be created in your data warehouse.

TO define a new materialization, create a `.sql` file in the `models` directory. The name of the file will be the name of the table created in your data warehouse.

```sql
-- myfirstmodel.sql
select 1 as test
```

Will create a `view` called `myfirstmodel.sql` in the default dataform schema defined in the [`dataform.json`](/docs/configuration/#dataform.json) file.

There are several configuration options that can be applied to a materialization. These can be applied by calling the appropriate method within a `${}` block, or can be provided all as one using the [options syntax](#Options syntax).

## Type

You can set the type of materialization to either `"view", "table", "incremental"` using the following syntax:

```js
${type("table")}
select 1 as test
```

## Options syntax

You can set several properties at once using the options syntax. The options method takes a JavaScript object as its argument:

```js
${options({
  type: "table",
  pre: "insert current_timestamp() into run_logs",
  dependencies: ["myothermodel"]
})}
select 1 as test
```
