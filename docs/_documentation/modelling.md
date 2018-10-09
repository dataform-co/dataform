---
layout: documentation
title: Modelling
sub_headers: ["Materializations"]
---

Modelling
===

# Materializations

The simplest way to define a new dataset, or materialization is to create a `.sql` file in the `models` directory. The name of the file will be the name of the table created in your data warehouse.

```sql
-- myfirstmodel.sql
select 1 as test
```

Will create a `view` by default in the default dataform schema defined in the [`dataform.json`](/docs/configuration/#dataform.json) file.

There are many configuration options that can be applied to materializations.

## Type

You can set the type of materialization to either `"view", "table", "incremental"` using the following syntax:

```js
${type("table")}
select 1 as test
```

# Materializations
