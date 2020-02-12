---
title: Declaring external datasets
priority: 4
---

## Introduction

Your Dataform code will likely depend on at least one external (i.e. not produced by Dataform) dataset.
Declarations enable you to treat these external datasets as first-class Dataform objects.

## Example

Suppose we have an external dataset `input.data`.

To declare this dataset you can use SQLX:

```js
config {
  type: "declaration",
  schema: "input",
  name: "data"
}
```

Alternatively you can use the JavaScript API:

```js
declare({
  schema: "input",
  name: "data"
});
```

## Dependencies on declared datasets

Once declared, a dataset can be referenced or resolved in the same way as any other dataset in Dataform.

Assuming the following declaration of the `input.data` dataset:

```js
config {
  type: "declaration",
  schema: "input",
  name: "data"
}
```

Other actions may now use the `ref()` or `resolve()` functions as usual, e.g. `ref("data")` or `resolve({schema: "input", name: "data"})`.
