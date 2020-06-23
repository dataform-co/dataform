---
title: Selecting Specific Columns in Google BigQuery
subtitle: Learn how to select all columns, except some, within Bigquery
priority: 2
---

When working with your data, it's sometimes easy to forget that you don't need to work with* all \_of it, \_all* of the time.

You've probably encountered situations where you have specific fields in your tables that are common across them, but they're not relevant to your query, so why include them?

For example, you could have a department table with columns `deptno,` `deptname` and `location`, but you could exclude location as it's not relevant to your analysis.

So, how do you do this in BigQuery? (and why should you?)

## Your Data & BigQuery

Understanding how your data is represented in BigQuery can help you to improve the quality, performance, and costs associated with your queries.

When you load your data into BigQuery, each column is stored separately. The values within each column are then compressed, and the corresponding data file is replicated, before being stored in an underlying distributed filesystem.

This process and representation of your data is why BigQuery doesn't support indices, and why, crucially, each column you pull in increases the cost. Each additional column requires access to a _different_ file (one of the replications) in the underlying filesystem.

With traditional databases, an index can often be the right solution. When using BigQuery however, as the size of your data increases more servers are utilised to keep performance at a consistent level. Due to this, you should expect your costs to increase linearly, while performance stays the same without the need for indexes.

This makes it quite different from traditional databases but makes sense when you realise that each column's data is essentially stored separately.

## Using SELECT \* EXCEPT (x,y,z)

Since the introduction of standard SQL support to BigQuery, you can now select specific columns, by _excluding_ specified columns from your SELECT statements. Here’s an example showing how you can exclude a few fields from your query using the EXCEPT function:

`select * except(title, comment) from publicdata.samples.wikipedia limit 10`

It's as simple as that!

## Next Steps

Hungry to learn more about BigQuery?

Check-out our guides on how to [<u>export usage logs</u>](https://dataform.co/blog/exporting-bigquery-usage-logs), [<u>build a machine learning pipelin</u>](https://dataform.co/blog/bq-ml-pipeline)e or [<u>send data to Intercom</u>](https://dataform.co/blog/bigquery-to-gcf).

Or, if you'd like to learn more about us, check out [<u>our documentation</u>](https://docs.dataform.co/) and [<u>sign up for a free account</u>](https://app.dataform.co/) today!
