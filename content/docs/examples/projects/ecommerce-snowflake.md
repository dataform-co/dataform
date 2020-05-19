---
title: Ecommerce store on Snowflake
subtitle: A fictional ecommerce store project on a Snowflake data warehouse
priority: 2
---

# Summary

This project transforms four raw datasets (`CUSTOMER`, `NATION`, `LINEITEM` and `ORDERS`) into three summary reporting tables.

- `CUSTOMER_STATS` provides a single view of all customers: how many orders they’ve made, how much they’ve spent, where they’re from.
- `ORDER_STATS` provides a single view of every order, adding the number of line-items it included.
- `DAILY_COUNTRY_STATS` provides an aggregate view of total daily order volumes by country.

# Dependency tree of the project

<img src="https://assets.dataform.co/docs/sample_projects/snowflake_sample_project_dag.png"  width="1100"  alt="Sample Snowflae Dataform project DAG" />
<em>Dependency tree of the Snowflake sample project</em>

# View the project

<a href="https://github.com/dataform-co/dataform-example-project-snowflake" target="_blank"><button>See the example on GitHub</button></a>

<a href="https://app.dataform.co/#/6478728478588928/overview" target="_blank"><button intent="primary">See the example on Dataform web</button></a> (Viewing the example project on Dataform web requires sign up)
