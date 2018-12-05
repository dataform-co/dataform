---
layout: documentation
title: Materialization reference
---

A `materialization` defines a table, or view that will be created in your data warehouse.

For examples and usage, check out the [materializations guide](/guides/materializations).

{% assign methods = site.data.reference.methods %}

{% include method.md method=methods.ref %}

{% include method.md method=methods.self %}

{% include method.md method=methods.dependencies %}

{% include method.md method=methods.type %}

{% include method.md method=methods.where %}

{% include method.md method=methods.protected %}

{% include method.md method=methods.disabled %}

{% include method.md method=methods.preops %}

{% include method.md method=methods.postops %}

{% include method.md method=methods.partitionBy %}

{% include method.md method=methods.config %}
