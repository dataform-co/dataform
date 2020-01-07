---
title: Getting Started
priority: 0
---

## What is Dataform?

Dataform makes it easy to manage complex SQL pipelines in your data warehouse. Using Dataform's API you can power large, complex data transformations with just a few simple statements.

## What can it do?

Using Dataform you can:

- [Publish datasets and views](datasets)
- [Write assertions for your data](assertions)
- [Execute arbitrary SQL operations](operations)

Dataform currently supports Google BigQuery, Postgres, Amazon Redshift, Snowflake, and Azure SQL Data Warehouse.

## How can I use it?

You can get started quickly using <a target="_blank" rel="noopener" href="https://dataform.co">Dataform Web</a>, or if you prefer, check out our open source [command line interface](dataform-cli) to develop Dataform projects yourself.

In addition to providing a nice graphical user interface, Dataform Web integrates many useful additional cloud services, making it the perfect data management tool for larger teams.

## How does it work?

1. You write SQL files enriched with Dataform's API and templating functions.
2. Dataform compiles, validates, and executes the generated SQL statements against your warehouse, automatically adding boilerplate such as `CREATE TABLE` and `INSERT` statements.
3. Clean, well-defined datasets are created in your data warehouse, which the rest of your team can use for anything from dashboards to machine learning.

Dataform's enriched SQL format allows you to:

- Reference and declare dependencies between datasets
- Re-use common SQL across any number of queries
- Write assertions against your data
- Document your dataset fields
- Write custom functions in JavaScript
