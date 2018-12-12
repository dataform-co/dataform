---
layout: documentation
title: JS API
---

Generally the easiest way to define [materializations](/guides/materializations), [assertions](/guides/assertions), and [operations](/guides/operations) is to create a new file with the appropriate extension, one of `.sql .assert.sql .ops.sql`.

For advanced use cases, all of the above can be defined via Dataform's a JavaScript API.

To use the JS API, create a `.js` file anywhere in the `definitions/` folder of your project.

The JS API provides three primary functions that correspond to the types of objects above:
- [`materialize()`](/reference/js-api/#materialize)
- [`operate()`](/reference/js-api/##operate)
- [`assert()`](/reference/js-api/##assert)

These are regular JavaScript (ES6) files that can contain arbitrary code, for loops, functions, constants etc.

## Example

Define an incremental table, provide a query and a where clause using a [`Contextable`](/reference/contextable) argument and `self()`.

```js
materialize("example_incremental")
  .type("incremental")
  .query("select 1 as ts")
  .where(context => `ts > (select max(ts) from ${context.self()})`);
```
