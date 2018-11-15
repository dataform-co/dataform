---
layout: documentation
title: Operations reference
---

An `operation` defines a set of SQL commands that will be executed in order against your warehouse, and can be used for running arbitrary queries.

For examples and usage, check out the [operations guide](/guides/operations).

{% assign methods = site.data.reference.methods %}

{% include method.md method=methods.ref context="true" %}

{% include method.md method=methods.dependencies context="true" %}
