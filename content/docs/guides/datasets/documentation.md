---
title: Document datasets
subtitle: Learn how to add data documentation for your tables and views.
priority: 3
---

## Introduction

Dataform allows you to add documentation to the datasets defined in your project.

## Adding table and field descriptions

Table and field descriptions are added using the config block.

```js
config {
  type: "table",
  description: "This table defines something",
  columns: {
    column1: "A test column",
    column2: "A test column",
    record1: {
      description: "A struct",
      columns: {
        column3: "A nested column"
      }
    }
  }
}

```

Alternatively you can use the JavaScript API:

```js
publish("table1", {
  type: "table",
  description: "This table defines something",
  columns: {
    column1: "A test column",
    column2: "A test column",
    record1: {
      description: "A struct",
      columns: {
        column3: "A nested column",
      },
    },
  },
});
```
