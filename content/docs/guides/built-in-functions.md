---
title: Built-in functions
---

## Introduction

When writing `.sqlx` files, Dataform makes a number of built-in functions available.

## `ref()`

`ref()` enables you to easily reference another dataset in your project without having to provide the full SQL dataset name. `ref()` also adds the referenced dataset to the set of dependencies for the query.

An example of `ref()` being used to add a dependency is [here](https://docs.dataform.co/guides/datasets/#referencing-other-datasets).

## `resolve()`

`resolve()` works similarly to `ref()`, but doesn't add the dataset to the dependency list for the query.

## `self()`

`self()` returns the name of the current dataset. If the default schema or dataset name is overridden in the `config{}` block, `self()` will return the full and correct dataset name.

An example of `self()` being used to set up incremental tables is [here](/guides/incremental-datasets/#a-simple-example).
