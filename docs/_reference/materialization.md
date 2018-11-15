---
layout: documentation
---

{% include method.md method=site.data.reference.methods.ref context="true" %}

## Example

```js
select * from ${ref("sourcetable")}
```
```js
select * from "schema"."sourcetable"
```
