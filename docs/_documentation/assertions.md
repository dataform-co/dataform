---
layout: documentation
title: Assertions
---

# Assertions

An `assertion` is a query that should return 0 rows, and can be used to check the state of your materializations as part of your pipelines.

To define a new assertion, create a `.assert.sql` file in the `models` directory.

The following assertion checks that the combination of 3 fields in a table are always unique:
```js
select id, a, b, sum(1) as count
from ${ref("sometable")}
group by id, a, b
having count > 1
```

## Context functions

Assertion files (`.assert.sql`) can use the following built-ins:

- [ref()](/built-in-functions/#ref)
- [dependencies()](/built-in-functions/#dependencies)
