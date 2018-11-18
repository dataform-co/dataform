---
layout: documentation
title: JS API reference
---

Materializations, operations, and assertions can all be defined in JavaScript, for more more advanced use cases. The following methods are globally available, or can be explicitly required from `"@dataform/core"`.

{% assign methods = site.data.reference.methods %}

{% include method.md method=methods.materialize %}

{% include method.md method=methods.operate %}

{% include method.md method=methods.assert %}
