---
title: Environments
subtitle: Learn about environments in Dataform web and how to configure them
---

## Introduction

By default, Dataform runs all of your project code from your project's `master` Git branch. Configuring environments allows you to control this behaviour, enabling you to run multiple different versions of your project code.

**An environment is effectively a wrapper around a version of your project code.** Once you have defined an environment, and added some schedules to that environment, Dataform runs all of those schedules using the environment's version of the project code.

A common use-case for environments is to run a staged release process. After testing code in a `staging` environment, the code is promoted to a stable `production` environment.

<callout intent="info">
  If you would like to use environments, please verify that the version of <code>@dataform/core</code> that your project uses is at least <code>1.4.9</code>.
</callout>

<br />

<callout intent="info">
  Environments are an advanced feature. To get the most out of them, the <b>use of an external Git provider is recommended</b>. This makes it easier to view specific commit SHAs, manage Git branches and tags, and control your release process.
</callout>

## Configuring environments

Environments are configured in your Dataform project's `environments.json` file.

An environment consists of:

- a name
- a Git reference at which the code should be run (one of: a branch name, tag name, or a commit SHA)
- (optionally) overridden values for the project's configuration (the settings in `dataform.json`)
- (optionally) some schedules

<callout intent="warning">
If an environment is configured using a tag or branch, that tag or branch must exist in the project's remote Git repository (e.g. GitHub, if the project is stored there). Note that <b>Dataform branches are not equivalent to remote Git branches</b>.
</callout>

A simple example of an `environments.json` file is:

```json
// environments.json
{
  "environments": [
    {
      "name": "staging",
      "gitRef": "67bed6bd4205ce97fa0284086ed70e5bc7f6dd75",
      "configOverride": {
        "defaultDatabase": "dataform_staging"
      },
      "schedules": [
        {
          "name": "run_everything_once_per_day",
          "cron": "0 10 * * *"
        }
      ]
    }
  ]
}
```

This `staging` environment runs the `run_everything_once_per_day` schedule at the `67bed6bd...` Git commit SHA. It also overrides the value of
`defaultDatabase` to isolate `staging` schedule runs from those in other environments.

Note that Dataform uses the `environments.json` file on your `master` branch to determine your project's environments. **Any changes to your environments must be pushed to `master` before Dataform will take note of those changes**.

### Code that depends on environment/schedule name

Dataform injects two special [variables](/guides/configuration#configure-custom-compilation-variables) when schedules are executed: `environmentName` and `scheduleName`. You can use these in your code by referencing `dataform.projectConfig.vars`. For example, to select 10% of data in a `staging` environment:

```js
// definitions/my_view.sqlx
config { type: "view" }

select
  *
from ${ref("data")}
${when(
  dataform.projectConfig.vars.environmentName === "staging",
  "where farm_fingerprint(id) % 10 = 0",
)}
```

## Example: manage a production release process using environments

More advanced Dataform projects typically have a tightly-controlled `production` environment. All changes to project code go into a `staging` environment which is intentionally kept separate from `production`. Once the code in `staging` has been verified to be sufficiently stable, that version of the code is then promoted to the `production` environment.

Note that a `staging` environment is typically not useful for code development. Usually during development you would simply run all code using the project's default settings (as defined in `dataform.json`). Thus, unless you want Dataform to run schedules in a `staging` environment, it may not be useful to define one.

A clean and simple way to separate staging and production data is to use a different database for each environment. However, this is only supported for BigQuery and Snowflake, so we recommend using per-environment schema suffixes for other warehouse types. The examples below show how to do both by overriding project configuration settings.

### Configuration via commit SHA

In the below example:

- the `production` environment is locked to a specific Git commit

- the `staging` environment runs the project's schedules at the latest version of the project's code (as exists on the `master` branch)

A nice property of this configuration is that any change to the `production` environment leaves an audit trail (by being recorded in your project's Git history).

```json
// environments.json
{
  "environments": [
    {
      "name": "production",
      "gitRef": "PRODUCTION_GIT_COMMIT_SHA_GOES_HERE",
      "configOverride": {
        "defaultDatabase": "dataform_production"
      },
      "schedules": [ ... ]
    },
    {
      "name": "staging",
      "gitRef": "master",
      "configOverride": {
        "defaultDatabase": "dataform_staging"
      },
      "schedules": [ ... ]
    }
  ]
}
```

To update the version of the project running in `production`, change the value of `commitSha`, and then push that change to your `master` branch. On GitHub, Git commit SHAs can be found by opening the project page and clicking 'commits'.

<callout>
  <p>A Git branch or tag can be specified instead of a commit SHA:</p>
  <p>
  <code>"gitRef": "GIT_BRANCH_OR_TAG_NAME_GOES_HERE"</code>
  </p>

<p>
However, we do not recommend using Git branches to manage a <code>production</code> environment:

  <ul>
    <li>Managing merges across Git branches can be tricky and occasionally produces confusing results</li>
    <li>Consequently, the audit trail of changes made to the environment can be harder to follow</li>
  </ul>
</p>
</callout>

### Per-environment schema suffixes

An alternative approach to separating production and staging data is to append a suffix to schemas in each environment. For example:

```json
// environments.json
{
  "environments": [
    {
      "name": "production",
      "gitRef": "PRODUCTION_GIT_COMMIT_SHA_GOES_HERE",
      "configOverride": {
        "schemaSuffix": "_production"
      },
      "schedules": [ ... ]
    },
    {
      "name": "staging",
      "gitRef": "master",
      "configOverride": {
        "schemaSuffix": "_staging"
      },
      "schedules": [ ... ]
    }
  ]
}
```

## Example: use separate databases for development and production data

Some teams may not be at the stage where they require a `staging` environment, but still would like to keep development and production data separated. This can be done using a `configOverride` in the `production` environment.

In the example below:

- any code deployed during development will use `defaultDatabase` from the `dataform.json` file
- schedules are defined in the `production` environment, and so use the `defaultDatabase` from that environment's `configOverride`
- the `production` environment specifies the `master` Git branch, so all of its schedules will run using the latest version of the code

```json
// dataform.json
{
  "warehouse": "bigquery",
  "defaultSchema": "dataform_data",
  "defaultDatabase": "analytics-development"
}
```

<br />

```json
// environments.json
{
  "environments": [
    {
      "name": "production",
      "gitRef": "master",
      "configOverride": { "defaultDatabase": "analytics-production" },
      "schedules": [ ... ]
    }
  ]
}
```

Note that the `defaultDatabase` setting is only supported for BigQuery and Snowflake. For other warehouses, we recommend overriding schema suffixes (as described above).
