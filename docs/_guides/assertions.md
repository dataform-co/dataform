---
layout: documentation
title: Assertions
---

An `assertion` is a query that should return 0 rows, and can be used to check the state of your materializations as part of your pipelines.

To define a new assertion, create a `.assert.sql` file in the `models` directory.

## Example

The following assertion checks that 3 fields in a table are never null:

```js
select * from ${ref("sometable")}
where a is not null
  and b is not null
  and c is not null
```

## Reference

Check the [assertions reference](/reference/assertions) for a list of all methods you can use in `.assert.sql` files.
