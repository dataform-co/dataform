---
title: Use JavaScript files
subtitle: Learn how to define several actions in a single Javascript file
priority: 2
---

## Introduction

Dataform provides a JavaScript API for defining all actions via code instead of creating individual SQL files for each action. This can be useful in advanced use cases.

To use the JavaScript API, create a `.js` file anywhere in the `definitions/` folder of your project. This code will be run during project compilation.

The JavaScript API exposes functions that create any of the actions that you would otherwise define in a SQL file:

- Datasets
- Declarations
- Assertions
- Operations
- Tests

These are regular JavaScript (ES5) files that can contain arbitrary code: loops, functions, constants, etc.

## Defining actions in JavaScript

The following file is a simple example of creating actions in JavaScript:

`definitions/example.js`:

```js
publish("table1").query("SELECT 1 AS test");

declare({
  schema: "rawdata",
  name: "input1",
});

assert("assertion1").query("SELECT * FROM source_table WHERE value IS NULL");

operate("operation1").queries("INSERT INTO some_table (test) VALUES (2)");

test("test1")
  .dataset("some_table")
  .input("input_data", "SELECT 1 AS test")
  .expect("SELECT 1 AS test");
```

This example creates a dataset, a declaration, an assertion, an operation, and a test in a single file, which if written in SQL would have required 5 seperate files.

## Setting properties on actions

Each of the global methods - `publish()`, `declare()`, `operate()`, `assert()`, and `test()` - return an object that can be used to configure that action. The API follows a builder syntax which can be seen in the following example:

```js
publish("table1")
  .query("SELECT 1 AS test") // Defines the query
  .type("table") // Sets the query's type
  .dependencies(["other_table"]) // Specifies dataset dependencies
  .descriptor({
    test: "Value is 1", // Describes fields in the dataset
  });
```

Multiple configuration properties can also be set using the `config()` method, or alternatively simply passed as a second argument to the method:

```js
publish("table1", {
  type: "table",
  dependencies: ["other_table"],
  descriptor: {
    test: "Value is 1",
  },
});
```

Learn more about configuration options for each type of action:

- [`publish()` (Datasets)](datasets)
- [`declare()` (Declarations)](declarations)
- [`assert()` (Assertions)](assertions)
- [`operate()` (Operations)](operations)
- [`test()` (Tests)](tests)

## Using built-in functions

When writing `.sqlx` files, Dataform makes a number of built-in functions such as `ref()` and `self()` available to use within the main query. For example:

```js
// definitions/example.sqlx
config { type: "view" }
SELECT * FROM ${ref("other_table")}
```

The `ref()` function is made available for the script to use automatically. Note that this is not the case when using the JavaScript API.

To use these functions in JavaScript, API methods - such as `query()` - take a `Contextable` argument. Instead of providing a string as the argument to the API method,
you can pass a function whose only parameter is a `context` object. This object exposes the built-in functions for JavaScript code to use.

For example, the above example written in JavaScript (making handy use of JavaScript template strings), in `definitions/example.js`:

```js
publish("example").query((ctx) => `SELECT * FROM ${ctx.ref("other_table")}`);
```

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Note that when using JavaScript template strings - which allow use of the <code>${"{}"}</code>{" "}
  syntax - the string in question must be wrapped in backticks <code>`</code> instead of single{" "}
  <code>'</code> or double <code>"</code> quotes.
</div>

<br />

The following methods and configuration options accept a function taking a `Contextable` argument as in the above example:

- `query()`
- `where()`
- `preOps()`
- `postOps()`

## Example: Dynamic dataset generation

One of the most common use cases for using the JavaScript API is to perform a similar action repeatedly.

For example, imagine you have several datasets, all of which have a `user_id` field. Perhaps you would like to create a view of each dataset with certain blacklisted user IDs removed.
You could perform this action across all datasets using a JavaScript `forEach()` statement:

```js
// definitions/blacklist_views.js
const datasetNames = ["user_events", "user_settings", "user_logs"];

datasetNames.forEach((datasetNames) => {
  publish(datasetNames + "_blacklist_removed").query(
    (ctx) => `
      SELECT * FROM ${ctx.ref(tableName)}
      WHERE user_id NOT IN (
        SELECT user_id
        FROM ${ctx.ref("blacklisted_user_ids")}
      )`
  );
});
```

This script would create 3 new datasets as views named `user_events_blacklist_removed`, `user_settings_blacklist_removed`, and `user_logs_blacklist_removed` that don't contain any of the blacklisted user IDs.
