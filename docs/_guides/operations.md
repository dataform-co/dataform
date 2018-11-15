---
layout: documentation
title: Operations
---

# Operations

An `operation` defines a set of SQL commands that will be executed in order against your warehouse, and can be used for arbitrary operations that don't necessarily create materializations or tests.

To define a new operation, create a `.ops.sql` file in the `models` directory.

For example, the following file defines two queries that will be run in order and executes vacuum commands in `Redshift`:
```js
vacuum delete only sales to 75 percent
---
vacuum reindex listing
```

Multiple statements can be seperated with a single line containing only `---`

## Context functions

Operation files (`.ops.sql`) can use the following built-ins:

- [ref()](/built-in-functions/#ref)
- [dependencies()](/built-in-functions/#dependencies)
