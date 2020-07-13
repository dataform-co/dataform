---
title: Best practices to maximise efficiency
subtitle: Follow best practices to build a project that will scale and maximise  your team efficiency over time
priority: 2
---

## Choose the right action type

SQLX supports three types of materializations depending on your needs: tables, views and incremental tables. As general rules:

- Use `views` by default (they’re cheap and quick)
- Use `tables` for datasets you expect to have downstream users for performance reasons. For example datasets queries by BI tools or datasets that have multiple descendents.
- If the input data is very large, use `incremental` tables. They transform only new data (but are more complex to manage)

## Organise data for your users using custom schemas

Dataform has a default schema defined in your dataform.json file. You can override it in the config block of your SQLX files.

```sql
-- definitions/staging/stg_customer.sqlx
config {
  type: "view",
  schema: "staging",
  tags: ["staging", "daily"],
  description: "Cleaned version of the raw customer table."
}

select
  c_custkey as customer_key,
  c_name as full_name,
  ...
```

## Test the quality of your data with assertions

Assertions are data quality checks, used to ensure data meets expectations.

Assertions are run as part of your schedule: if an assertion fails, you will be notified (if notifications are configured)

<img src="https://assets.dataform.co/docs/best_practices/assertions_dag.png"  alt="" />

Assertions can be added to the config block in your .sqlx files. There are three types:

- `uniqueKey`: ensure there is only one row per value of the supplied column(s)
- `nonNull`: ensure the field(s) are not null
- `rowConditions`: takes a custom SQL expression. If this expression is FALSE for any rows, the assertion fails.

```sql
-- definitions/analytics/order_stats.sqlx

config {
  type: "table",
  assertions: {
    nonNull: ["order_date", "order_key", "customer_key"],
    uniqueKey: ["order_key"],
    rowConditions: [
      "total_parts >= 0"
    ]
  }
}

select ...
```

Note: you can also create custom assertions using SQLX. Read the [page on assertions](guides/assertions) for more.

## Document your data

Help keep collaboration on your project frictionless by adding documentation to your Dataform code.

You can add table and columns descriptions in the config block of SQLX files.

```sql
-- definitions/analytics/order_stats.sqlx

config {
  type: "table",
  description: "This table contains summary stats by date aggregated by country",
  columns: {
    order_date: "Date of the order",
    order_id: "ID of the order",
    customer_id: "ID of the customer in the CRM",
    order_status: "Status of the order, from Shopify",
    payment_status: "Status of payment, from Stripe",
    payment_method: "Credit card of ACH",
    item_count: "Number of items in that order",
    amount: "Amount charged for that order, in US dollars using a floating FX rate"
  }
}

select ...
```

Documentation is automatically added to the Data Catalog within Dataform and can later be exported to other tools.

<img src="https://assets.dataform.co/docs/best_practices/catalog_example.png"  alt="" />

## Keep your code DRY using reusable `Includes` macros

DRY stands for Don’t Repeat Yourself. JavaScript files can be added to the includes/ folder to define simple scripts, constants or macros that can be reused across your project.

Defining macros within includes allows you to keep your transformation logic DRY: write a function or variable once, and use it across the rest of your project

**Example: country group mapping**
The function country_group defines a mapping from country to region. It can be defined once, and then reused throughout your project. If the grouping changes, you only need to change code in one place

```sql
-- includes/mapping.js
function country_group(country){
  return `
  case
    when ${country} in ('US', 'CA') then 'NA'
    when ${country} in ('GB', 'FR', 'DE', 'IT', 'PL', 'SE') then 'EU'
    when ${country} in ('AU') then ${country}
    else 'Other'
  end`;
```

<br />

```sql
-- definitions/new_table.sqlx
config { type: "table"}

select
  country as country,
  ${mapping.country_group("country")} as country_group,
...
```

<br />

```sql
-- compiled.sql
select
  country as country,
  case
    when country in ('US', 'CA') then 'NA'
    when country in ('GB', 'FR', 'DE', 'IT', 'PL', 'SE') then 'EU'
    when country in ('AU') then country
    else 'Other'
  end as country_group,
...
```
