---
title: Re-use code with includes
subtitle: Learn how to re-use code across your project with includes
priority: 1
---

## Introduction

JavaScript files can be added to the `includes/` folder to define simple scripts, constants or macros that can be reused across your project.
Each file in the includes folder will be made available for use within your other SQL or JavaScript files.

It's also possible to add JavaScript to a `.sqlx` file by wrapping it in a `js {...}` block.
Note: Functions, constants or macros defined in this way will only be available within the .sqlx file they are defined in, not across the whole project.

If you are new to JavaScript, the examples below should cover some common use cases.
<a target="_blank" rel="noopener" href="https://developer.mozilla.org/en-US/docs/Web/JavaScript/A_re-introduction_to_JavaScript">MDN</a> is a useful learning resource if you'd like to learn more.

## Example: Defining and using constants

Create a new file in your project under the `includes/` folder, such as:

```js
// includes/constants.js
const PROJECT_ID = "my_project_name";
module.exports = { PROJECT_ID };
```
<br />
<callout intent="warning">
  Note that in order to use functions or constants elsewhere in the project, they must be exported
  using the <code>module.exports = {"{}"}</code> syntax.
</callout>

## Example: Using an include

You can reference any `include` function, constant, or macro by using its file name without the `.js` extension, followed by the name of the exported function or constant.

For example, to reference the constant `PROJECT_ID` in the file `includes/constants.js`:

```js
// definitions/query.sqlx
SELECT * FROM ${constants.PROJECT_ID}.my_schema_name.my_table_name
```

The query will be compiled into the following SQL before it is run:

```js
SELECT * FROM my_project_name.my_schema_name.my_table_name
```

## Example: Writing functions

Functions enable you to reuse the same block of SQL logic across many different scripts. Functions take 0 or more named parameters and must return a string.

In the example below, the function `countryGroup()` takes as input the name of the country code field and returns a CASE statement that maps country codes to country groups.

```js
// includes/country_mapping.js
function countryGroup(countryCodeField) {
  return `CASE
          WHEN ${countryCodeField} IN ("US", "CA") THEN "NA"
          WHEN ${countryCodeField} IN ("GB", "FR", "DE", "IT", "PL") THEN "EU"
          WHEN ${countryCodeField} IN ("AU") THEN ${countryCodeField}
          ELSE "Other countries"
          END`;
}

module.exports = { countryGroup };
```

This function can be used in a SQLX file:

```js
// definitions/revenue_by_country_group.sqlx
SELECT
  ${country_mapping.countryGroup("country_code")} AS country_group,
  SUM(revenue) AS revenue
FROM my_schema.revenue_by_country
GROUP BY 1

```

The query will be compiled into the following SQL before it is run:

```js
SELECT
  CASE
    WHEN country_code IN ("US", "CA") THEN "NA"
    WHEN country_code IN ("GB", "FR", "DE", "IT", "PL") THEN "EU"
    WHEN country_code IN ("AU") THEN country_code
    ELSE "Other countries"
  END AS country_group,
  SUM(revenue) AS revenue
FROM my_schema.revenue_by_country
GROUP BY 1
```

## Example: An include with parameters: groupBy

The example below defines a `groupBy()` function that takes as input a number of fields to group by and generates a corresponding `GROUP BY` statement:

```js
// includes/utils.js
function groupBy(n) {
  var indices = [];
  for (var i = 1; i <= n; i++) {
    indices.push(i);
  }
  return `GROUP BY ${indices.join(", ")}`;
}

module.exports = { groupBy };
```

This function can be used in a SQL query `definitions/example.sqlx`:

```js
SELECT field1,
       field2,
       field3,
       field4,
       field5,
       SUM(revenue) AS revenue
FROM my_schema.my_table
${utils.groupBy(5)}
```

The query will be compiled into the following SQL before it is run:

```js
SELECT field1,
       field2,
       field3,
       field4,
       field5,
       SUM(revenue) AS revenue
FROM my_schema.my_table
GROUP BY 1, 2, 3, 4, 5
```

## Example: Generating queries

Functions can be used to generate entire queries. This is a powerful feature that can be useful if you need to create several datasets which share a similar structure.

The example below includes a function that aggregates all metrics (using `SUM`) and groups by every dimension.

```js
// includes/script_builder.js
function renderScript(table, dimensions, metrics) {
  return `
      SELECT
      ${dimensions.map((field) => `${field} AS ${field}`).join(",\\n")},
      ${metrics.map((field) => `SUM(${field}) AS ${field}`).join(",\\n")}
      FROM ${table}
      GROUP BY ${dimensions.map((field, i) => `${i + 1}`).join(", ")}
    `;
}
module.exports = { renderScript };
```

This function can be used in a SQL query `definitions/stats_per_country_and_device.sqlx`:

```js
${script_builder.renderScript(
  ref("source_table"),
  ["country", "device_type"],
  ["revenue", "pageviews", "sessions"])}

```

Note that calls to functions such as `ref()` should be made in the SQL file itself and passed to the include function so that dependencies are configured correctly.

The query will be compiled into the following SQL before it is run:

```js
SELECT country AS country,
       device_type AS device_type,
       SUM(revenue) AS revenue,
       SUM(pageviews) AS pageviews,
       SUM(sessions) AS sessions
FROM my_schema.source_table
GROUP BY 1, 2
```
