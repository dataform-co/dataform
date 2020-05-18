---
title: ELT and the modern data stack
subtitle: An introduction to ELT and where Dataform fits in
priority: 1
---

# From ETL to ELT

The traditional ETL, which stands for Extraction, Transformation, and Loading, has now evolved onto ELT. Data is Extracted from source systems, Loaded into the data warehouse and then Transformed within the data warehouse. Dataform helps you manage that last part, the transformation in your data warehouse.

# A simple ELT example

Imagine you want to build the data stack of an ecommerce shop. This business has three data sources: Shopify for the web store, Stripe to process payments, and Salesforce as their CRM. You want to use all that data to build reports to track KPIs, create dashboards and conduct ad hocs analysis to understand the business. One of the fist tasks would be to create a dashboard in a BI tool with all your customers information for everyone in the company to know about your customers. This dashboard will have all the data you have about your customers.

<img src="https://assets.dataform.co/docs/introduction/example_simple_schema.png" max-width="753"  alt="" />

In this example, you want to use all the data you have (coming from Shopify, Stripe, and Salesforce) to create a unified dashboard.

## The data warehouse

The data warehouse is the epicenter of modern stacks. Raw data from across the company is centralized in the warehouse. Data is transformed in the warehouse. BI and analytics tools read data from the warehouse.

<img src="https://assets.dataform.co/docs/introduction/datastack_simple_schema.png" max-width="819"  alt="" />

Most businesses today will use cloud data warehouses like Google BigQuery, Snowflake, or AWS Redshift.

## Extraction and Loading

The first step in building a data stack is to Extract raw data from all sources and load it in the data warehouse. You can achieve this with third party tools, or by writing custom scripts.

<img src="https://assets.dataform.co/docs/introduction/elt_illustration_step1.png" max-width="1100"  alt="" />

The data loaded in your warehouse is raw and unprocessed. Each of those sources will generate dozens of tables in your data warehouse. At this stage, the data is not really usable for analytics. Answering simple questions like **“Which customers order the most products?"** would probably take several hours and writing complex queries.

## Transform the data

The next step is to transform the data. You want to turn the hundreds of tables of raw data loaded in your warehouse into a single source of truth that will represent your business.

For our example of creating a customer dashboard, you will want to join the data from the different sources together, normalize the fields and filter bad data to create a unique customers table. That table will contain all the information you have about your customers and will let you answer questions like “Which customers order the most products?” very quickly.

<img src="https://assets.dataform.co/docs/introduction/elt_illustration_step2.png" max-width="1100"  alt="" />

This customers table will be the table you will use for your dashboards. That means that everyone in the company will see that data and rely on it to make decisions. As a result this data needs to be tested so that it can be trusted. It needs to be refreshed frequently so your dashboards have the latest information. It needs to be documented for everyone to know what the different fields mean.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" markdown="1">
<h4 class="bp3-heading">This is the step that Dataform helps you complete</h4>
Dataform helps you and your team turn the raw data in your warehouse into a suite of data tables to represent your business.

It helps you build tables that are **well defined**, **tested** and **documented** to power your entire analytics.</a></div>

## Use transformed data in your analytics

After your data is transformed, you can use BI and other analytics tools to build dashboards and conduct analysis.

<img src="https://assets.dataform.co/docs/introduction/elt_illustration_step3.png" max-width="1100" alt="" />
