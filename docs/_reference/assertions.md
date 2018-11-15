---
layout: documentation
title: Assertions reference
---

An `assertion` is a query that should return 0 rows, and can be used to check the state of your materializations as part of your pipeline.

For examples and usage, check out the [assertions guide](/guides/assertions).

{% assign methods = site.data.reference.methods %}

{% include method.md method=methods.ref context="true" %}

{% include method.md method=methods.dependencies context="true" %}
