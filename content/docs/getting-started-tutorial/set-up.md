---
title: Getting set up
subtitle: Learn how to create a new BigQuery project and generate warehouse credentials.
priority: 0
---

Welcome to the Dataform Getting Started Tutorial. This tutorial is for people who are new to Dataform and would like to learn how to set up a new project. We will show you how to create your own data model, how to test and document it and how to configure schedules. A working knowledge of SQL will be helpful for this tutorial.

For this tutorial we’re going to pretend we are a fictional e-commerce shop. We already have 3 main data sources in our data warehouse:

- Information about our customers coming from Salesforce
- Orders information from Shopify
- Payment information from Stripe

The aim of this tutorial is to create two new tables in our warehouse, one called `order_stats` and one called `customers`, which are:

- Updated every hour
- Tested for data quality
- Well documented

<div style="position: relative; padding-bottom: 55.78124999999999%; height: 0;"><iframe src="https://www.loom.com/embed/2368b67928ec43b2a7eaf8fabda636f9" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

## Create a new BigQuery project

Dataform connects to [many different warehouses](https://docs.dataform.co/warehouses) but for this tutorial we’ll use BigQuery, since anyone with a Google Account can use it and it has a free tier. We’ve created a public dataset in BigQuery that anyone can access for the purpose of this tutorial.

1. First you will need to create a new Bigquery project:

- Go to the [BigQuery console](https://console.cloud.google.com/bigquery) (If you don’t already have a GCP account you’ll need to create one here).
- If you’ve just created a new account you’ll be asked to create a new project straight away. If you already have an existing account you can select the project drop down in the header bar, and create a new project from there.

<img src="https://assets.dataform.co/getting%20started%20tutorial/set%20up/Screenshot%202020-08-13%20at%2015.40%201%20(1).png" max-width="753"  alt="" />

## Generate warehouse credentials

In order for Dataform to connect to your BigQuery warehouse you’ll need to generate some credentials. Dataform will connect to BigQuery using a service account. You’ll need to create a service account from your Google Cloud Console and assign it permissions to access BigQuery.

1. To create a new service account in Google Cloud Console you need to:

   - Go to the [Services Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)
   - Make sure the new project you created is selected and click `Open`.

- Click on `Create Service Account` and give it a name.
- Grant the new account the BigQuery Admin role.

2. Once you’ve done this you need to create a key for your new service account (in JSON format):

- On the [Service Accounts page](https://console.cloud.google.com/iam-admin/serviceaccounts), find the row of the service account that you want to create a key for and click the `More` button.
- Then click `Create key`.
- Select JSON key type and click `Create`.

Now you've created a new BigQuery project and generated your warehouse credentials, you're ready to create your Dataform project!

For more detailed info on generating credentials for BigQuery, see our [docs](https://docs.dataform.co/warehouses/bigquery).
