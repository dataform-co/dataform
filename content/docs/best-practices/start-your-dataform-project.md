---
title: First steps with your Dataform project
subtitle: Principles to start your project on the right foot
priority: 1
---

## Organise your project with folders

Managing your project in folders makes it easier to maintain your project. One simple option is to follow the following folder structure:

<img src="https://assets.dataform.co/docs/best_practices/folder_example.png"  alt="" />

<figcaption>Example of a Dataform project</figcaption>

**Sources**

Where you define transformations on your data sources. This is where you can create datasets that transform data from your different sources into a consistent format, using the same structure and naming conventions. Some examples of transformations you may want to include at this stage are:

- Normalising sources (ex: ensure email have the same field name in all tables)
- Aligning data types (ex: ensure timestamps are in a unique time zone and money fields in dollars)

All subsequent datasets should be built on top of these models, reducing the amount of duplicated code in your project.

**Staging**

Where you can define intermediary datasets that are only used within your Dataform project. The objectives of these tables are to make your project easier to maintain, easier to reason about, and make your pipelines more efficient.

**Analytics**

Where you define datasets that represent entities and processes relevant to your business and will be consumed by downstream users (BI tools, reports…).

As your project gets bigger and more complex, you can leverage subdirectories within those folders to separate groups of sources or analytics tables.

## Manage dependencies with `ref()`

The `ref()` function ensures data transformation pipelines run in the correct sequence. We recommend you always use the `ref()` function when selecting or joining from another dataset rather than using the direct relation reference.

```sql
-- definitions/table_without_ref.sqlx
config { type: "table" }

select * from "schema"."my_table"
```

<br />

```sql
-- definitions/table_with_ref.sqlx
config { type: "table" }

select * from ${ref("my_table)}
```

The `ref` function is how you tell Dataform about the dependencies in your project. With this information, Dataform builds a dependency tree of your project in real time, and achieves the following things:

- Alerts you in in real time about dependency errors
- Ensures you are in the correct environment
- Run your transformation pipelines in the correct sequence

## Define your source data with declarations

Your Dataform project will depend on raw data stored in your warehouse, created by processes external to Dataform. These external processes can change the structure of your tables over time (column names, column types…).

We recommend defining raw data as declarations to build your projects without any direct relation reference to tables in your warehouse.

<img src="https://assets.dataform.co/docs/best_practices/declarations_dag.png"  alt="" />

Using declarations enables you to reference your raw data in a unique place that can be updated in your data sources changes.

There are two ways to define declarations: in SQLX files and in JS files.

In SQLX files, with one declaration per file.

```sql
-- definitions/sources/customer.sqlx
config {
  type: "declaration",
  database: "SNOWFLAKE_SAMPLE_DATA",
  schema: "TPCH_SF1",
  name: "CUSTOMER",
}
```

In JS files, where you can define multiple declarations in one file. You can use this to define all raw data from the same source for example.

```js
// definitions/sources/stripe_dependencies.js
declare({
  schema: "stripe",
  name: "charges"
});

declare({
  schema: "stripe",
  name: "accounts"
});

declare({
  schema: "stripe",
  name: "..."
});
```

Once a declaration has been defined, it can be referenced by the ref() function like any other Dataform dataset.

```sql
-- definitions/staging/charges.sqlx
config { type: "table" }

select * from ${ref("charges")}
```

## Use schedules to run transformations at a regular interval

Schedules update the datasets defined by your project in your data warehouse at a specified frequency

For example, you may set up a schedule to rerun all transformations once per hour. You can create multiple schedules, if you require several different frequencies.

If you are using Dataform web, schedules are defined in your `environment.js` file and are version controlled. See the page on [scheduling runs](/dataform-web/scheduling) to learn how to configure schedules.
