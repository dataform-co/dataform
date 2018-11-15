---
layout: documentation
method:
  name: dependencies()
  signature: dependencies(deps)
  args:
    - name: deps
      type: 'string | string[]'
      description: "Either a single dependency name, or a list"
---

{% include method.md method=page.method %}

Specifies one or more materializations, operations or assertions that this node depends on.
Supports wildcard matches with `"*"`.

```js
${dependencies("sourcetable")}
```

Multiple tables can be provided in a single call:

```js
${dependencies(["sourcetable", "othertable"])}
```
