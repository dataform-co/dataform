---
title: Set up a warehouse
---

## Introduction

In order to connect to your data warehouse to run your project's compiled SQL on your behalf, Dataform requires some warehouse access details.

Configuring your data warehouse settings should take less than 5 minutes. If you encounter any difficulty, please contact our team using Intercom or via email at [team@dataform.co](mailto:team@dataform.co).

## BigQuery

### Create a service account

You’ll need to create a service account from your Google Cloud Console and assign it permissions to access BigQuery.

1. Follow <a target="_blank" rel="noopener" href="https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating_a_service_account">these instructions</a> to create a new service account in Google Cloud Console.
2. Grant the new account the `BigQuery Admin` role. (Admin access is required by Dataform so that it can create queries and list tables.) Read
   <a target="_blank" rel="noopener" href="https://cloud.google.com/iam/docs/granting-roles-to-service-accounts#granting_access_to_a_service_account_for_a_resource">this</a> if you need help.
3. Create a key for your new service account (in JSON format). You will upload this file to Dataform. Read
   <a target="_blank" rel="noopener" href="https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating_service_account_keys">this</a> if you need help.

## Postgres (running in AWS) or Redshift

Postgres and Redshift projects require the following configuration settings:

- Hostname in the form `[name].[id].[region].redshift.amazonaws.com`
- Port (usually `5432` for Postgres or `5439` for Redshift)
- Username and password
- Database name

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Dataform's IP addresses must be whitelisted in order to access your Redshift cluster. Please
  follow{" "}
  <a
    target="_blank"
    rel="noopener"
    href="https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-authorize-cluster-access.html"
  >
    these instructions
  </a>
  . Dataform's IP addresses are <code>35.233.106.210</code> and <code>104.196.10.242</code>.
</div>

### How to find Redshift credentials

1. Go to `Redshift` in your AWS console.
2. Select your cluster under `Clusters`.
3. The hostname is the endpoint listed at the top of the page. Username and database name are listed under cluster database properties.
   You may prefer to create a separate username and password for Dataform to use - please contact our team if you need help.

## Snowflake

Snowflake connections require the following elements:

- Account name: the first part of your Snowflake url, including region: `account-name.region`.snowflakecomputing.com
- Username and password used for your Snowflake console. You may prefer to create a separate username and password for Dataform to use - please contact our team if you need help.
- Warehouse name: Click "Warehouses" from within your Snowflake console to view a list of warehouses. Any warehouse in your account will work with any database.
- Database: Click "Databases" from within your Snowflake console to view a list of databases.

You may also need to whitelist Dataform's IP addresses: `35.233.106.210` and `104.196.10.242`
