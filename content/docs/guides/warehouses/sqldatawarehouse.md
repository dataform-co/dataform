---
title: Azure SQL Data Warehouse
---

Azure SQL Data Warehouse specific options can be applied to tables using the `sqldatawarehouse` configuration parameter.

## Setting table distribution

SQL Data Warehouse supports table <a target="_blank" rel="noopener" href="https://docs.microsoft.com/en-us/azure/sql-data-warehouse/massively-parallel-processing-mpp-architecture#distributions">distributions</a>.
Manually tuning this parameter can help to optimize query performance.

SQL Data Warehouse distributions can be configured in Dataform using the `distribution` option:

```js
config {
  type: "table",
  sqldatawarehouse: {
    distribution: "REPLICATE"
  }
}
SELECT CURRENT_TIMESTAMP() AS ts
```

This query compiles to the following statement:

```js
CREATE TABLE "dataform"."example"
WITH (distribution = REPLICATE)
AS SELECT CURRENT_TIMESTAMP() AS ts
```
