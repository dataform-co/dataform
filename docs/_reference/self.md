---
layout: documentation
method:
  name: self()
  signature: self()
  returns: "The full, query-able name of the current materialization"
---

{% include method.md method=page.method %}

Returns a full table reference to the current materialization output table. Useful for incremental table builds.

```js
${type("incremental")}
${where(`ts > (select max(ts) from ${self()})`)}
select now() as ts
```
