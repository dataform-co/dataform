---
title: Tags
---

## Introduction

Tags allow you to easily refer to a collection of actions. This can be useful in creating schedules and triggering runs.

## Example

Suppose we would like to add a "daily" tag to a dataset.

To add a tag to a dataset:

```js
config {
  type: "table",
  name: "users",
  tags: ["daily"]
}
```

Alternatively you can use the JavaScript API:

```js
publish("users")
  .query("SELECT ...")
  .tags(["daily"]);
```

Tags can be added to datasets, assertions, operations and declarations. You can add more than one tag to each action.

## Using tags

Once you have assigned tags to actions, you can use the tags to define a run or [schedule](../dataform-web/guides/scheduling) . For example, in the schedule creation
screen, simply choose the tags you'd like to include in that schedule.

Tags can also be used with the CLI using the `--tags` argument.
