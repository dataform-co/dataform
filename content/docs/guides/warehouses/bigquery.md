---
title: Google BigQuery
---

BigQuery specific options can be applied to tables using the `bigquery` configuration parameter.

## Setting table partitions

BigQuery supports <a target="_blank" rel="noopener" href="https://cloud.google.com/bigquery/docs/partitioned-tables">partitioned tables</a>.
These can be useful when you have data spread across many different dates but usually query the table on only a small range of dates.
In these circumstances, partitioning will increase query performance and reduce cost.

BigQuery partitions can be configured in Dataform using the `partitionBy` option:

```js
config {
  type: "table",
  bigquery: {
    partitionBy: "DATE(ts)"
  }
}
SELECT CURRENT_TIMESTAMP() AS ts
```

This query compiles to the following statement which takes advantage of BigQuery's DDL to configure partitioning:

```js
CREATE OR REPLACE TABLE dataform.example
PARTITION BY DATE(ts)
AS (SELECT CURRENT_TIMESTAMP() AS ts)
```

If desired, partitions can be clustered by using the `clusterBy` option, for example:

```js
config {
  type: "table",
  bigquery: {
    partitionBy: "DATE(ts)",
    clusterBy: ["name", "revenue"]
  }
}
SELECT CURRENT_TIMESTAMP() as ts, name, revenue
```

## Configuring access to Google Sheets

In order to be able to query Google Sheets tables via BigQuery, you'll need to share the sheet with the service account that is used by Dataform.

- Find the email address of the service account you connected Dataform with by looking in the `.df-credetials.json` file locally, or by finding the account from the [Google Cloud IAM service accounts console](https://console.cloud.google.com/iam-admin/serviceaccounts).
- Share the Google sheet with the email address of the service account as you would a colleage, through the sheets sharing settings and make sure it has access.
