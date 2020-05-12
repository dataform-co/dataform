---
title: Schedule runs
priority: 2
---

## Introduction

Schedules can be used to run any selection of your datasets at a user-specified frequency. Running your scripts on a repeating schedule ensures output data is always kept up to date.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Schedules
  <b>
    only run from the <code>production</code> branch
  </b> (if there is no environment configured)
  . Make sure you commit and push your changes to the production branch after configuring schedules.
</div>

## Create a schedule

1. Navigate to the Scheduling page using the left menu
2. Click `Add schedule`
3. Enter your schedule's settings and click `Create schedule`

![Create schedule](/static/images/how_to_guides/scheduling/create_schedule.png)

## View past runs

You can find all current and historical scheduled runs in the _Run logs_ page, accessible using the left menu bar.

![Run logs](/static/images/how_to_guides/scheduling/run_logs.png)

## Scheduler access tokens

For projects using a remote git project (e.g. hosted on GitHub), a scheduler access token is required. [More info on configuring git access tokens](./git-access-tokens).
