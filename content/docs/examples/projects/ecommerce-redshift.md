---
title: Ecommerce store on Redshift
subtitle: A fictional ecommerce store project on a  Redshift data warehouse
priority: 3
---

# Summary

This project transforms four raw data tables (`orders` from Shopify, `charges` from Stripe, and `customer` from a CRM) into three summary reporting tables.

- `order_stats` brings all orders information into a single table
- `customer_stats` brings all customer information into a single table
- `daily_country_stats` provides an overview of stats aggregated by country

# Dependency tree of the project

<img src="https://assets.dataform.co/docs/sample_projects/redshift_sample_project_dag.png"  width="1100"  alt="Sample bigquery Dataform project DAG" />
<em>Dependency tree of the Redshift sample project</em>

# View the project

<a href="https://github.com/dataform-co/dataform-example-project" target="_blank"><button>See the example on GitHub</button></a>

<a href="https://app.dataform.co/#/6470156092964864/overview"><button intent="primary">See the example on Dataform web</button></a> (Viewing the example project on Dataform web requires sign up)
