---
layout: documentation
title: Materialization reference
---

A `materialization` defines a table, or view that will be created in your data warehouse.

For examples and usage, check out the [materializations guide](/guides/materializations).

{% assign methods = site.data.reference.methods %}

{% include method.md method=methods.ref context="true" %}

{% include method.md method=methods.self context="true" %}

{% include method.md method=methods.dependencies context="true" %}

{% include method.md method=methods.type context="true" %}

{% include method.md method=methods.where context="true" %}

{% include method.md method=methods.protected context="true" %}

{% include method.md method=methods.preops context="true" %}

{% include method.md method=methods.postops context="true" %}

{% include method.md method=methods.partitionBy context="true" %}

{% include method.md method=methods.config context="true" %}
