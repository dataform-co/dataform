---
title: Amazon Redshift
---

Redshift specific options can be applied to datasets using the `redshift` configuration parameter.

## Distributing data

You can configure how Redshift <a target="_blank" rel="noopener" href="https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html#t_data_distribution_concepts">distributes data</a> in your cluster by configuring the `distStyle` and `distKey` properties.

```js
config {
  type: "table",
  redshift: {
    distKey: "user_id",
    distStyle: "key"
  }
}
SELECT user_id FROM ...
```

This query compiles to the following statement:

```js
CREATE TABLE "dataform"."example"
DISTKEY(user_id)
DISTSTYLE even
AS SELECT user_id FROM ...
```

## Sorting data

You can also configure how Redshift <a target="_blank" rel="noopener" href="https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html">sorts data</a> in your datasets with the `sortKeys` and `sortStyle` properties.

```js
config {
  redshift: {
    sortKeys: [ "ts" ],
    sortStyle: "compound"
  }
}
SELECT 1 AS ts
```

# Binding views

By default, all views in Redshift are created as late binding views. This can be changed by setting the `bind` property in the redshift configuration block.

```js
config {
  type: "view",
  redshift: {
    bind: true
  }
}
SELECT 1 AS ts
```
