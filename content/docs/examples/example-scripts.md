---
title: Example scripts
subtitle: A list of examples of scripts to use in your Dataform projects
priority: 1
---

## Basic examples

### Create a view

```sql
config { type: "view" }

select * from source_data
```

### Create a table

```sql
config { type: "table" }

select * from source_data
```

### Use the ref function

```sql
config { type: "table" }

select * from ${ref("source_data")}
```

### Run several SQL operations

```sql
config { type: "operations" }

delete from datatable where country = 'GB'
---
delete from datatable where country = 'FR'
```

### Add documentation to a table, view, or declaration

```sql
config { type: "table",
         description: "This table is an example",
         columns:{
             user_name: "Name of the user",
             user_id: "ID of the user"
         } }

select user_name, user_id from ${ref("source_data")}
```

### Add assertions to a table, view, or declaration

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

### Add a custom assertion

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

### Run custom SQL before or after creating a table

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

## Incremental tables examples

### Add new rows dates for new dates in source data

```sql
config { type: "incremental" }

select date(timestamp) as date, action
from weblogs.user_actions

${ when(incremental(), `where timestamp > (select max(date) from ${self()})`)
```

### Take a snapshot of a table periodically

```sql
config { type: "incremental" }

select date(timestamp) as date, action
from weblogs.user_actions

${ when(incremental(), `where timestamp > (select max(date) from ${self()})`)
```

## Examples of assertions - data quality tests

```sql
config { type: "incremental" }

SELECT current_date() AS snapshot_date, customer_id, name, account_settings FROM productiondb.customers

${ when(incremental(), `where snapshot_date > (select max(snapshot_date) from ${self()})`) }
```

## Examples of includes

### Use global variables

```js
// includes/contants.js
const project_id = "project_id";
const first_date = "'1970-01-01'";
module.exports = {
  project_id,
  first_date
};
```

<br />

```sql
-- definitions/new_table.sqlx
config {type: "table"}

select * from source_table where date > ${contants.first_date}
```

### Create a country mapping

```sql
-- includes/mapping.js
function country_group(country){
  return `
  case
    when ${country} in ('US', 'CA') then 'NA'
    when ${country} in ('GB', 'FR', 'DE', 'IT', 'PL', 'SE') then 'EU'
    when ${country} in ('AU') then ${country}
    else 'Other'
  end`;
```

<br />

```sql
-- definitions/new_table.sqlx
config { type: "table"}

select
  country as country,
  ${mapping.country_group("country")} as country_group,
  device_type as device_type,
  sum(revenue) as revenue,
  sum(pageviews) as pageviews,
  sum(sessions) as sessions

from ${ref("source_table")}

group by 1, 2, 3
```

<br />

```sql
-- compiled.sql
select
  country as country,
  case
    when country in ('US', 'CA') then 'NA'
    when country in ('GB', 'FR', 'DE', 'IT', 'PL', 'SE') then 'EU'
    when country in ('AU') then country
    else 'Other'
  end as country_group,
  device_type as device_type,
  sum(revenue) as revenue,
  sum(pageviews) as pageviews,
  sum(sessions) as sessions

from "dataform"."source_table"

group by 1, 2, 3
```

### Generate a SQL script with a custom function

```js
// includes/script_builder.js
function render_script(table, dimensions, metrics) {
  return `
      select
      ${dimensions.map(field => `${field} as ${field}`).join(",")},
      ${metrics.map(field => `sum(${field}) as ${field}`).join(",\n")}
      from ${table}
      group by ${dimensions.map((field, i) => `${i + 1}`).join(", ")}
    `;
}

module.exports = { render_script };
```

<br />

```sql
-- definitions/new_table.sqlx
config {
    type: "table",
    tags: ["advanced", "hourly"],
    disabled: true
}

${script_builder.render_script(ref("source_table"),
                               ["country", "device_type"],
                               ["revenue", "pageviews", "sessions"]
                               )}
```

<br />

```sql
-- compiled.sql
select
  country as country,
  device_type as device_type,
  sum(revenue) as revenue,
  sum(pageviews) as pageviews,
  sum(sessions) as sessions

from "dataform"."source_table"

group by 1, 2
```

## Examples using the JS API

### Generating one table per country

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

### Declaring multiple sources within one file

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

### Deleting sensitive information in all tables containing PII

```js
const pii_tables = ["users", "customers", "leads"];
pii_tables.forEach(table =>
  operate(`gdpr_cleanup: ${table}`,
    ctx => `
      delete from raw_data.${table}
      where user_id in (select * from users_who_requested_deletion)`)
      .tags(["gdpr_deletion"]))
);

```

## Misc

### Use inline variables and functions

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

### Perfom a unit test on a SQL query

```sql
-- definitions/query_to_be_tested.sqlx
select
  floor(age / 5) * 5 as age_group,
  count(1) as user_count
from ${ref("source_table")}
group by age_group
order by age_group
```

<br />

```sql
-- definitions/unit_test_on_query.sqlx
config {
  type: "test",
  dataset: "source_table"
}

input "ages" {
  select 15 as age union all
  select 21 as age union all
  select 24 as age union all
  select 34 as age
}

select 15 as age_group, '1' as user_count union all
select 20 as age_group, '2' as user_count union all
select 30 as age_group, '1' as user_count
```

### Backfill a daily table

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
operate(`create table`, 'create table if not exists backfill_table (`fields`) `);
// step 2: insert into the table

dateArr.forEach((day, i) =>
  operate(`backfill ${day}`
   `insert into backfill_table select fields where day = '${day}'`)
);
```

### Build a rolling 30-days table that update incrementally

```sql
-- definitions/incremental_example.sql
config {type: "incremental"}

postOperations {
  delete from ${self()} where date < (date_add(Day, -30, CURRENT_DATE))
}

select
 date(timestamp) as date,
 order_id,
from source_table
  ${ when(incremental(), `where timestamp > (select max(date) from ${self()})`) }
```
