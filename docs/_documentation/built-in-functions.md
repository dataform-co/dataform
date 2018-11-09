---
layout: documentation
title: Built-in functions
sub_headers: ["ref()", "dependencies()", "self()", "pre()", "post()", "assert()", "type()", "where()"]
---

# Built-in functions

Build in functions are made available within the context of `.sql` files.
They can be used with the following syntax:

```js
${functionName(...)}
```

## `ref()`

References another materialization in the project, and adds that materialization as a [dependencies](#dependencies) of the current materialization, operation, or test.

Arguments: `model-name`

Returns: full table reference

For example, the following query:

```js
select * from ${ref("sourcetable")}
```

Gets compiled to something (depending on the warehouse type) like:

```js
select * from "schema"."sourcetable"
```
And has the side affect of adding the `sourcetable` materialization as a dependencies.

## `dependencies()`

Specifies one or more materializations, operations or assertions that this node depends on.
Supports wildcard matches with `"*"`.

```js
${dependencies("sourcetable")}
```

Multiple tables can be provided in a single call:

```js
${dependencies(["sourcetable", "othertable"])}
```

## `self()`

Returns a full table reference to the current materialization.

## `preOps()`

Allows you to specify statements that should be executed before the main materialization statement.

## `postOps()`

Allows you to specify statements that should be executed after the main materialization statement.

## `assert()`

Allows you to specify tests inline as part of a materialization.

## `type()`

Changes the type of the materialization. See [materializations](/materializations) for more details.

## `where()`

Specifies the where clause used for incremental. See [incremental tables](/materializations#incremental-tables) for more details.
