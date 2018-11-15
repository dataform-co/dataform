---
layout: documentation
method:
  name: ref()
  signature: ref(materializationName)
  args:
    - name: materializationName
      type: string
      description: "The name of the node to reference"
  returns: "The full, query-able name of the referenced table"
---

{% include method.md method=page.method %}

References another materialization in the project by it's name. This is either the name of the file preceeding `.sql`, or the name explciticly given when defining materializations via [materialize()](/reference/materialize).

## Example

```js
select * from ${ref("sourcetable")}
```
```js
select * from "schema"."sourcetable"
```
