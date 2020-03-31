---
title: Environments
---

## Introduction

By default, Dataform runs all of your project code directly off your project's `master` Git branch. Configuring environments allows you to control this behaviour, enabling you to run multiple different versions of your project code.

**An environment is effectively a wrapper around a version of your project code (including the `schedules.json` file)**. Once you have defined an environment, Dataform runs all of the schedules defined inside that environment 'wrapper', using that version of the project code.

A common use-case for environments is to run a staged release process. Code is first run in a `staging` environment, and after being sufficiently tested, is then later pushed to a `production` environment.

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  If you would like to use environments, please verify that the version of <code>@dataform/core</code> that your project uses is at least <code>1.4.9</code>.
</div>

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  Environments are an advanced feature. To get the most out of them, the <b>use of an external Git provider is recommended</b>. This makes it easier to view specific commit SHAs, manage Git branches, and control your release process.
</div>

## Configuring environments

Environments are configured in your Dataform project's `environments.json` file.

An environment consists of:

- a name
- a Git reference at which the code should be run (either a branch name or a commit SHA)
- (optionally) overridden values for the project's configuration (the settings in `dataform.json`)

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
If an environment is configured using a branch, that branch must exist on the project's remote Git repository (e.g. GitHub, if the project is stored there). Note that <b>Dataform branches are not remote Git branches.</b>
</div>

A simple example of an `environments.json` file is:

```json
{
  "environments": [
    {
      "name": "staging",
      "gitReference": {
        "commitSha": "67bed6bd4205ce97fa0284086ed70e5bc7f6dd75"
      },
      "configOverride": {
        "defaultDatabase": "dataform_staging"
      }
    }
  ]
}
```

This `staging` environment runs the code (and schedules) at the `67bed6bd4205ce97fa0284086ed70e5bc7f6dd75` Git commit SHA. It also overrides the values of
`defaultDatabase` to isolate `staging` schedule runs from those in other environments.

Note that Dataform uses the `environments.json` file on your `master` branch to determine your project's environments. **Any changes to your environments must be pushed to `master` before Dataform will take note of those changes**.

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  If your project has a missing or empty <code>environments.json</code> file, Dataform uses a default environment which runs the code on your <code>master</code> branch with no configuration overrides.
</div>

## Example: manage a production release process using environments

More advanced Dataform projects typically have a tightly-controlled `production` environment. All changes to project code go into a `staging` environment which is intentionally kept separate from `production`. Once the code in `staging` has been verified to be sufficiently stable, that version of the code is then pushed into the `production` environment.

Note that, unless you want Dataform to run schedules in `staging`, it may not be a requirement to define an explicit `staging` environment. Instead, you could maintain a single `production` environment and simply run all code under development using the project's default settings (as defined in `dataform.json`).

A clean and simple way to separate staging and production data is to use a different database for each environment. However, this is only supported for BigQuery and Snowflake, so we recommend using per-environment schema suffixes for other warehouse types. The examples below show how to do both by overriding project configuration settings.

### Configuration via commit SHA

In the below example:

- the `production` environment is locked to a specific Git commit

- the `staging` environment runs the project's schedules at the latest version of the project's code (as exists on the `master` branch)

A nice property of this configuration is that any change to the `production` environment leaves an audit trail (by being recorded in your project's Git history).

```json
{
  "environments": [
    {
      "name": "staging",
      "gitReference": {
        "branch": "master"
      },
      "configOverride": {
        "defaultDatabase": "dataform_staging"
      }
    },
    {
      "name": "production",
      "gitReference": {
        "commitSha": "PRODUCTION_GIT_COMMIT_SHA_GOES_HERE"
      },
      "configOverride": {
        "defaultDatabase": "dataform_production"
      }
    }
  ]
}
```

To update the version of the project running in `production`, change the value of `commitSha`, and then push that change to your `master` branch. On GitHub, Git commit SHAs can be found by opening the project page and clicking 'commits'.

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  <p>A Git branch can be specified instead of a commit SHA: <code>"gitReference": { "branch": "GIT_BRANCH_NAME_GOES_HERE" }</code></p>

<p>
However, we do not recommend using Git branches to manage a <code>production</code> environment:

  <ul>
    <li>Managing merges across Git branches can be tricky and occasionally produces confusing results</li>
    <li>Consequently, the audit trail of changes made to the environment can be harder to follow</li>
  </ul>
</p>
</div>

### Per-environment schema suffixes

An alternative approach to separating production and staging data is to append a suffix to schemas in each environment. For example:

```json
{
  "environments": [
    {
      "name": "staging",
      "gitReference": {
        "branch": "master"
      },
      "configOverride": {
        "schemaSuffix": "_staging"
      }
    },
    {
      "name": "production",
      "gitReference": {
        "commitSha": "PRODUCTION_GIT_COMMIT_SHA_GOES_HERE"
      },
      "configOverride": {
        "schemaSuffix": "_production"
      }
    }
  ]
}
```

## Example: use separate databases for development and production data

Some teams may not be at the stage where they require a `staging` environment, but still would like to keep development and production data separated. This can be done using a `configOverride` in the `production` environment.

In the example below:

- any code deployed during development will use the `defaultDatabase` from the `dataform.json`
- any code deployed by schedules will be in the `production` environment and so use the `defaultDatabase` from the `configOverride` in `environmemts.json`
- schedules will use the version of the code from the `master` branch

`dataform.json`:
```json
{
    "warehouse": "bigquery",
    "defaultSchema": "dataform_data",
    "defaultDatabase": "analytics-development",
}
```


`environments.json`:
```json
 {
    "environments": [
        {
            "name": "production",
            "gitReference": {
                "branch": "master"
            },
            "configOverride": {"defaultDatabase": "analytics-production"}
        }
    ]
}
```

Note, this is only supported for BigQuery and Snowflake. For other warehouses, we recommend overriding schema suffixes instead of `defaultDatabase`.
