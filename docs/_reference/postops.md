---
layout: documentation
method:
  name: postOps()
  signature: postOps(ops)
  args:
    - name: ops
      type: 'Contextable<string | string[]>'
      description: "The queries to run"
---

{% include method.md method=page.method %}

Provide one of more queries to execute after this materialization has completed.
The argument is contextable, meaning it can be a function that takes the current context as the argument.
