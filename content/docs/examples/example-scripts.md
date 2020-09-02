---
title: Example scripts
subtitle: A list of examples of scripts to use in your Dataform projects.
priority: 1
---

## Basic examples

### Create a view

```sql
-- definitions/new_view.sqlx
config { type: "view" }

select * from source_data
```

### Create a table

```sql
-- definitions/new_table.sqlx
config { type: "table" }

select * from source_data
```

### Use the ref function

```sql
-- definitions/new_table_with_ref.sqlx
config { type: "table" }

select * from ${ref("source_data")}
```

### Run several SQL operations

```sql
-- definitions/operations.sqlx

config { type: "operations" }

delete from datatable where country = 'GB'
---
delete from datatable where country = 'FR'
```

### Add documentation to a table, view, or declaration

```sql
-- definitions/documented_table.sqlx

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
-- definitions/tested_table.sqlx

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
-- definitions/custom_assertion.sqlx

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
-- definitions/table_with_preops_and_postops.sqlx

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
-- definitions/incremental_table.sqlx

config { type: "incremental" }

select date(timestamp) as date, action
from weblogs.user_actions

${ when(incremental(), `where timestamp > (select max(date) from ${self()})`)
```

### Take a snapshot of a table periodically

```sql
-- definitions/snapshots_table.sqlx

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

### Extracting browser and platform from Segment's context_user_agent field

```js
// includes/segment.js

// source: https://discourse.looker.com/t/parsing-user-agent-into-device-type-manufacturer-browser/1206/2

function platformStart(user_agent) {
  return `STRPOS(${user_agent}, '(') + 1`;
}

function platformRaw(user_agent) {
  return `SUBSTR(${user_agent}, ${platformStart(user_agent)}, 100)`;
}

function platformEndInitial(user_agent) {
  return `
    CASE
      WHEN STRPOS(${platformRaw(user_agent)}, ';') = 0
      THEN STRPOS(${platformRaw(user_agent)}, ')')
      ELSE STRPOS(${platformRaw(user_agent)}, ';')
    END`;
}

function platformEnd(user_agent) {
  return `
CASE WHEN ${platformEndInitial(user_agent)} = 0 THEN 0 ELSE ${platformEndInitial(user_agent)} - 1 END`;
}

function platform(user_agent) {
  return `SUBSTR(${user_agent}, ${platformStart(user_agent)}, ${platformEnd(user_agent)})`;
}

function browser(user_agent) {
  return `
    CASE
      WHEN ${user_agent} LIKE '%Firefox/%' THEN 'Firefox'
      WHEN ${user_agent} LIKE '%Chrome/%' OR ${user_agent} LIKE '%CriOS%' THEN 'Chrome'
      WHEN ${user_agent} LIKE '%MSIE %' THEN 'IE'
      WHEN ${user_agent} LIKE '%MSIE+%' THEN 'IE'
      WHEN ${user_agent} LIKE '%Trident%' THEN 'IE'
      WHEN ${user_agent} LIKE '%iPhone%' THEN 'iPhone Safari'
      WHEN ${user_agent} LIKE '%iPad%' THEN 'iPad Safari'
      WHEN ${user_agent} LIKE '%Opera%' THEN 'Opera'
      WHEN ${user_agent} LIKE '%BlackBerry%' AND ${user_agent} LIKE '%Version/%' THEN 'BlackBerry WebKit'
      WHEN ${user_agent} LIKE '%BlackBerry%' THEN 'BlackBerry'
      WHEN ${user_agent} LIKE '%Android%' THEN 'Android'
      WHEN ${user_agent} LIKE '%Safari%' THEN 'Safari'
      WHEN ${user_agent} LIKE '%bot%' THEN 'Bot'
      WHEN ${user_agent} LIKE '%http://%' THEN 'Bot'
      WHEN ${user_agent} LIKE '%www.%' THEN 'Bot'
      WHEN ${user_agent} LIKE '%Wget%' THEN 'Bot'
      WHEN ${user_agent} LIKE '%curl%' THEN 'Bot'
      WHEN ${user_agent} LIKE '%urllib%' THEN 'Bot'
      ELSE 'Unknown'
    END`;
}

module.exports = {
  platform,
  browser
}
```

<br />

```sql
// definitions/top_browser_platforms

config{
  type: "view"
}

select
  ${segment.browser("context_user_agent")} as browser,
  ${segment.platform("context_user_agent")} as platform,
  count(distinct user_id) as users
from
  javascript.pages
group by
  1,2
order by
  users desc
limit 20;
```

## Examples using the JS API

### Generating one table per country

```js
// definitions/one_table_per_country.js

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
// definitions/external_dependencies.js

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

### Declaring multiple sources within one file using `forEach`

```js
// definitions/external_dependencies.js

["charges", "subscriptions", "line_items", "invoices"].
  forEach(name => declare({
    schema: "stripe",
    name})
);
```

### Deleting sensitive information in all tables containing PII

```js
// definitions/delete_pii.js

const pii_tables = ["users", "customers", "leads"];
pii_tables.forEach(table =>
  operate(`gdpr_cleanup: ${table}`,
    ctx => `
      delete from raw_data.${table}
      where user_id in (select * from users_who_requested_deletion)`)
      .tags(["gdpr_deletion"]))
);

```


### Adding preOps and postOps using the JS API

```js
// definitions/pre_and_post_ops_example.js

publish("example")
  .query(ctx => `SELECT * FROM ${ctx.ref("other_table")}`)
  .preOps(ctx => `delete ${ctx.self()}`)
  .postOps(ctx => `grant select on ${ctx.self()} to role`)
```

## Misc

### Use inline variables and functions

```sql
-- definitions/script_with_variables.sqlx

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
-- definitions/backfill_daily_data.js

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
