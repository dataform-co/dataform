---
layout: documentation
title: Core concepts
---

At it's core, Dataform runs SQL queries against your warehouse, in the correct order. It also provides a framework to make it easy to develop queries and dependencies, provides templating so you can re-use code across different queries, and provides pre built packages that you can use within your project so you don't have to re-invent the wheel.

There are 3 main types of things you can do in dataform:

[Materialize data sets](/guides/materializations) - create a table, or view in your warehouse from a SQL query.

[Execute arbitrary queries](/guides/operations) - run arbitrary SQL queries against your warehouse.

[Run assertions against your data](/guides/assertions) - check that data conforms to certain rules.

## Directed graph

Dataform compiles your project and computes a directed, acyclic graph of all the queries that should be run, and makes sure to run them in the correct order.

## API

Dataform provides APIs to make it easy to perform common tasks, such as [creating a table]((/guides/materializations), or incrementally inserting new rows into a table.

## JS API

Dataform provides a way to define anything in dataform via JavaScript, check out the [JS API reference](/reference/js-api) for more info.
