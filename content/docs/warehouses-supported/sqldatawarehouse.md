---
title: Azure SQL Data Warehouse
---

# Authentification

Azure SQL data warehouse connections require the following elements:

- Server
- Port
- Username
- Password
- Database name

# Configuration options

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

# Getting help

If you are using Dataform web and are having trouble connecting to Azure SQL Data Warehouse, please reach out to us by using the intercom messenger at the bottom right.

If you have other questions, you can join our slack community and ask question to get help.

<a href="https://slack.dataform.co"><button>Join dataform-users on slack</button></a>
