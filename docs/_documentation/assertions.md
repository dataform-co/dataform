---
layout: documentation
title: Assertions
---

# Assertions

An `assertion` can define one or more queries that, under normal conditions.

The best way to think about assertions, are as queries that scan for rows that break some test. If the queries return any results, then the test is considered to have failed.

To define a new assertion, create a `.test.sql` file in the `models` directory.

For example, the following file defines two tests on a dataset, one to make sure that the combination of 3 fields are always unique, and one to check there are no null values present:
```js
select id, a, b, sum(1) as count
from ${ref("sometable")}
group by id, a, b
having count > 1
---
select id, a, b from ${ref("sometable")} where a is NULL or b is NULL
```

Multiple statements can be seperated with a single line containing only `---`

## Context functions

Assertion files (`.test.sql`) can use the following built-ins:

- [ref()](/docs/built-in-functions/#ref)
- [dependency()](/docs/built-in-functions/#dependency)
