---
title: Incremental datasets
priority: 6
---

## Introduction

Incremental datasets aren't rebuilt from scratch every time they run. Instead, only new rows are inserted (or merged) into the dataset according to the conditions you provide when configuring the dataset.

Dataform takes care of managing state, creating datasets, and generating `MERGE` statements for you.

### Example: Append only for performance

Web logs or analytics data are great use cases for incremental datasets. For these kinds of data sources you usually only want to process new records instead of having to reprocess old data.

### Example: Micro-batching for latency

A major benefit of incremental datasets is that pipelines complete more quickly and can therefore be run more frequently at lower cost, reducing downstream latency to consumers of these datasets.

### Example: Creating daily snapshots

A common incremental dataset use case is to create daily snapshots of an input dataset, for example to use in longitudinal analysis of user settings stored in a production database.

## Key concepts

Incremental datasets are inherently stateful and make use of `INSERT` statements. Thus, care must be taken to ensure that data is not erased or duplicated during insertion of new rows.

There are 2 parts to an incremental dataset that you must configure:

- The main query that selects all rows
- A `WHERE` clause that determines which subset of rows should be processed for each incremental run

### `WHERE` clause

The `WHERE` clause is applied to the main query and is used to ensure that only new data is added to the incremental dataset. For example:

```js
ts > (SELECT MAX(timestamp) FROM target_table)
```

## A simple example

Assuming there is a source dataset containing timestamped user action events called `weblogs.user_actions` which is streamed into our warehouse via some other data integration tool:

<table className="bp3-html-table bp3-html-table-striped .modifier" style="width: 100%;">
  <thead>
    <tr>
      <th>timestamp</th>
      <th>user_id</th>
      <th>action</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1502920304</td>
      <td>03940</td>
      <td>create_project</td>
    </tr>
    <tr>
      <td>1502930293</td>
      <td>20492</td>
      <td>logout</td>
    </tr>
    <tr>
      <td>1502940292</td>
      <td>30920</td>
      <td>login</td>
    </tr>
    <tr>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
  </tbody>
</table>

To make a simple incremental copy of this dataset, create a new file called `definitions/example_incremental.sqlx`:

```js
config { type: "incremental" }

SELECT timestamp, action
FROM weblogs.user_actions

${ when(incremental(), `WHERE timestamp > (SELECT MAX(timestamp) FROM ${self()})`) }
```

First the script sets the type of the dataset to `incremental`.

It then specifies a `WHERE` clause using the `when()` and `incremental()` functions:

```js
${ when(incremental(), `WHERE timestamp > (SELECT MAX(timestamp) FROM ${self()})`) }
```

This ensures that only rows from the source dataset with a <b>timestamp greater than the latest timestamp that has been processed so far</b> are selected in the incremental query.

Note that `self()` is used here in order to get the name of the current dataset. Thus the compiled `WHERE` clause will be expanded to:

```sql
timestamp > (SELECT MAX(timestamp) FROM default_schema.example_incremental)
```

This dataset may not exist in the warehouse yet. That's OK, because the `WHERE` clause will only be added to the final query if the dataset already exists and new data is being inserted into it.

<div className="pt-callout pt-icon-info-sign pt-intent-warning" markdown="1">
  Note that when data is inserted into an incremental dataset, only fields that already exist in the
  dataset will be written. To make sure that new fields are written after changing the query, the
  dataset must be rebuilt from scratch with the <code>--full-refresh</code> option (if using the
  Dataform tool on the command line) or with the <code>Run with full refresh</code> option in
  Dataform Web.
</div>

## A merge modification

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  Incremental merging requires <code>@dataform/core</code> version <code>1.4.21</code>.<br>
  Incremental merging is not current supported for Azure SQLDataWarehouse.
</div>

In order to modify the above table to only contain only the most recent user actions, a `uniqueKey` could be specified.

When an incremental update appears, if each unique key specified within a row matches that of existing data, then the row will be updated with the newly arriving values.

If the `user_id` and `action` columns are set as a `uniqueKey`s within the config, then old timestamps will not persist. In this scenario, the config would look like

```sql
config {
  type: "incremental",
  uniqueKey: ["user_id", "action"]
}
```

In order to optimise this merge on BigQuery an update partition filter can be set. An example config for only merging into records from the last 48 hours would be:

```sql
config {
  type: "incremental",
  uniqueKey: ["user_id", "action"],
  bigQuery: {
    partitionBy: "DATE(timestamp)",
    updatePartitionFilter:
        "timestamp >= timestamp_sub(current_timestamp(), interval 48 hour)"
  }
}
```

### Generated SQL

The SQL generated by the above example will depend on the warehouse type, but generally follow the same format.

If the dataset doesn't exist yet:

```js
CREATE OR REPLACE TABLE default_schema.example_incremental AS
  SELECT timestamp, action
  FROM weblogs.user_actions;
```

When incrementally inserting new rows:

```js
MERGE default_schema.example_incremental T
USING (
  SELECT timestamp, action
  FROM weblogs.user_actions
  WHERE timestamp > (SELECT MAX(timestamp) FROM default_schema.example_incremental) S
ON T.user_id = S.user_id AND T.action = S.action AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
WHEN MATCHED THEN
  UPDATE SET timestamp = S.timestamp, user_id = S.user_id, action = S.action
WHEN NOT MATCHED THEN
  INSERT (timestamp, user_id, action) VALUES (timestamp, user_id, action)
```

If no unique key is specified, then the merge condition (`T.user_id = S.user_id` in this example) is set as `false`, causing rows to always be inserted rather than merged.

## Daily snapshots with incremental datasets

Incremental datasets can be used to create a daily snapshot of mutable external datasets.

Assuming an external dataset called `productiondb.customers`, you could write the following incremental to create a daily snapshot of that data:

```js
config { type: "incremental" }

SELECT CURRENT_DATE() AS snapshot_date, customer_id, name, account_settings FROM productiondb.customers

${ when(incremental(), `WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM ${self()})`) }
```

- By selecting the current date as `snapshot_date`, this effectively appends a dated snapshot of the `productiondb.customers` dataset to the output dataset each day.
- The `WHERE` clause here prevents more than one insertion per day (which could result in duplicate rows in the output dataset), since any newly inserted rows must have a later `snapshot_date`
  than any previously existing row in the output.
- This Dataform project should be scheduled to run at least once a day.

## Protecting incremental datasets from data loss

It's possible to force an incremental dataset to be rebuilt from scratch using either the command line interface with the <code>--full-refresh</code> option or with the <code>Run with full refresh</code> option in Dataform Web.

If you need to protect a dataset from ever being rebuilt from scratch, for example if the source data is only temporary, you can mark an incremental dataset as `protected`.
This means that Dataform will never delete this dataset, even if a user requests a full refresh.

`definitions/incremental_example_protected.sqlx`:

```js
config {
  type: "incremental",
  protected: true
}
SELECT ...
```

## Table partitioning / dist keys

Since incremental datasets are usually timestamp based, it's a best practice to set up dataset partioning on the timestamp field to speed up downstream queries.

For more information, check out the [BigQuery](warehouses/bigquery) and [Redshift](warehouses/redshift) guides.
