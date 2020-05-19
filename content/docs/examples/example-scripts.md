---
title: Example scripts
subtitle: A list of examples of scripts to use in your Dataform projects
priority: 1
---

_This page is quite long, use the table of content on the right (or CTRL+F) to find what you are looking for_

# Basic examples

## Create a view

```sql
config { type: "view" }

select * from source_data
```

## Create a table

```sql
config { type: "table" }

select * from source_data
```

## Use the ref function

```sql
config { type: "table" }

select * from ${ref("source_data")}
```

## Run several SQL operations

```sql
config { type: "operations" }

delete from datatable where country = 'GB'
---
delete from datatable where country = 'FR'
```

## Add documentation to a table, view, or declaration

```sql
config { type: "table",
         description: "This table is an example",
         columns:{
             user_name: "Name of the user",
             user_id: "ID of the user"
         } }

select user_name, user_id from ${ref("source_data")}
```

## Add assertions to a table, view, or declaration

```sql
config {
  type: "table",
  assertions: {
    uniqueKey: ["user_id"],
    nonNull: ["user_id", "customer_id"],
    rowConditions: [
      'signup_date is null or signup_date > "2019-01-01"',
      'email like "%@%.%"'
    ]
  }
}
select ...
```

## Add a custom assertion

```sql
config { type: "assertion" }

select
  *
from
  ${ref("source_data")}
where
  a is null
  or b is null
  or c is null
```

## Run custom SQL before or after creating a table

```sql

config {type: "table"}

select * from ...

pre_operations {
  insert into table ...
}

post_operations {
  grant select on ${self()} to group "allusers@dataform.co"
  ---
  grant select on ${self()} to group "allotherusers@dataform.co"
}

```

# Examples of incremental tables

## Add new rows dates for new dates in source data

```sql
config { type: "incremental" }

select date(timestamp) as date, action
from weblogs.user_actions

${ when(incremental(), `where timestamp > (select max(date) from ${self()})`)
```

## Take a snapshot of a table periodically

```sql
config { type: "incremental" }

select date(timestamp) as date, action
from weblogs.user_actions

${ when(incremental(), `where timestamp > (select max(date) from ${self()})`)
```

# Examples of assertions - data quality tests

```sql
config { type: "incremental" }

SELECT current_date() AS snapshot_date, customer_id, name, account_settings FROM productiondb.customers

${ when(incremental(), `where snapshot_date > (select max(snapshot_date) from ${self()})`) }
```

# Examples of includes

## Use global variables

## Create a country mapping

## Generate a SQL script with a custom function

# Examples using the JS API

## Generating one table per country

```js
const countries = ["GB", "US", "FR", "TH", "NG"];

countries.forEach(country => {
  publish("reporting_" + country)
    .dependencies(["source_table"])
    .query(
      ctx => `
      select '${country}' as country
      `
    );
});
```

## Declaring multiple sources within one file

```js
declare({
  schema: "stripe",
  name: "charges"
});

declare({
  schema: "shopify",
  name: "orders"
});

declare({
  schema: "salesforce",
  name: "accounts"
});
```

## Deleting sensitive information in all tables containing PII

# Misc

## Use inline variables and functions

```sql
js {
 const foo = 1;
 function bar(number){
     return number+1;
 }
}

select
 ${foo} as one,
 ${bar(foo)} as two
```

## Perfom a unit test on a SQL query

## Backfill a daily table

```js
var getDateArray = function(start, end) {
  var startDate = new Date(start); //YYYY-MM-DD
  var endDate = new Date(end); //YYYY-MM-DD

  var arr = new Array();
  var dt = new Date(startDate);
  while (dt <= endDate) {
    arr.push(new Date(dt).toISOString().split("T")[0]);
    dt.setDate(dt.getDate() + 1);
  }
  return arr;
};

var dateArr = getDateArray("2020-03-01", "2020-04-01");

// step 1: create table

// step 2: insert into the table

dateArr.forEach((day, i) =>
  operate(`backfill ${day}`, `INSERT INTO table SELECT 1 AS test WHERE day = '${day}'`)
);
```

## Build a rolling 30-days table that update incrementally

## jean
