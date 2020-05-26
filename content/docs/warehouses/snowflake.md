---
title: Snowflake
subtitle: Authentification, configuration options, and content for Snowflake
priority: 0
---

## Authentification

Snowflake connections require the following elements:

- Account name: the first part of your Snowflake url, including region: `account-name.region`.snowflakecomputing.com
- Username and password used for your Snowflake console. You may prefer to create a separate username and password for Dataform to use - please contact our team if you need help.
- Warehouse name: Click "Warehouses" from within your Snowflake console to view a list of warehouses. Any warehouse in your account will work with any database.
- Database: Click "Databases" from within your Snowflake console to view a list of databases.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
You may also need to whitelist Dataform's IP addresses to let Dataform access your Snowflake instance. Please
  follow
  <a
    target="_blank"
    rel="noopener"
    href="https://docs.snowflake.com/en/user-guide/network-policies.html"
  >these instructions</a>. Dataform's IP addresses are <code>35.233.106.210</code> and <code>104.196.10.242</code>.
</div>

## Configuration options

### Querying different databases

You can both read from and publish to two separate snowflake databases within a single Dataform project. For example, you may have a database called `RAW_SOURCE` that contains raw data loaded in your warehouse and a database called `ANALYTICS` in which you create data tables you use for analytics and reporting.

You can define a default database in your `dataform.json` file.

```js
{
    "warehouse": "snowflake",
    "defaultSchema": "dataform",
    "assertionSchema": "dataform_assertions",
    "defaultDatabase": "ANALYTICS"
}

```

You can then override the default database in the config block of your SQLX files.

```js
config {
    type: "table",
    database: “RAW_SOURCE”
}

```

#### Using separate databases for development and production

You can configure separate databases for development and production in your `environment.json` file. The process is described on [this page](https://docs.dataform.co/dataform-web/scheduling/environments#example-use-separate-databases-for-development-and-production-data).

## Dataform web features for Snowflake

### Real time query validation

Dataform validates the compiled script you are editing against Snowflake in real time. It will let you know if the query is valid (or won’t run) before having to run it.

<video autoplay controls loop  muted  width="680" ><source src="https://assets.dataform.co/docs/compilation.mp4" type="video/mp4" ><span>Real time compilation video</span></video>

## Sample Dataform project with Snowflake

We prepared the following sample project of a fictional ecommerce store using using Snowflake.

<img src="https://assets.dataform.co/docs/sample_projects/snowflake_sample_project_dag.png" width="1100"  alt="Sample bigquery Dataform project DAG" />
<figcaption>Dependency tree of the Snowflake sample project</figcaption>

<a href="../examples/projects/ecommerce-snowflake"><button>View project page</button></a>

## Getting help

If you are using Dataform web and are having trouble connecting to Snowflake, please reach out to us by using the intercom messenger icon at the bottom right of the app.

If you have other questions related to Snowflake, you can join our slack community and ask question on the #Snowflake channel.

<a href="https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A"><button>Join dataform-users on slack</button></a>
