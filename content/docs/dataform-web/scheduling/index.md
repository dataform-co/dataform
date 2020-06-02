---
title: Schedule runs
subtitle: Learn how to schedule runs and view logs
priority: 2
---

## Introduction

Schedules can be used to run any selection of your datasets at a user-specified frequency. Running your code on a repeating schedule ensures output data is always kept up to date.

## Create a schedule

Schedules are created as part of an [environment](scheduling/environments).

1. Navigate to your `environments.json` file
2. Click `Edit` on the environment in which the schedule should run (by default you will only have a single `production` environment)
3. Click `Create new schedule`
4. Enter your schedule's settings
5. Commit your changes to `environments.json` and push these to `master`

## View past runs

You can find all current and historical scheduled runs in the _Run logs_ page, accessible using the left menu bar.

![Run logs](https://assets.dataform.co/docs/bigquery_billing.png)

## Scheduler access tokens

For projects using a remote git project (e.g. hosted on GitHub), a scheduler access token is required. [More info on configuring git access tokens](./git-access-tokens).
