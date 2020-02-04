---
title: Environments
---

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  Environments require <code>@dataform/core >= 1.4.9</code>.
</div>

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  Environments are an advanced feature. To get the most out of them, the <b>use of an external remote git provider is recommended</b>. This will make it easier to view specific commit SHAs, create remote git branches, create pull requests and code review, and manage a staging to production configuration.
</div>

## Introduction

Environments can be used to configure different variations of your schedules. **For each environment
you define, using that environment's version of the project code, Dataform will run all of the
project's schedules.**

A common use-case for environments is to run a staged release process; you might
want to push your code to a `staging` environment first, and then push it to a `production`
environment once the code has been tested.

## Configuring environments

Environments are configured in your Dataform project's `environments.json` file.

An environment consists of:

- a name
- a Git reference at which the code should be run (either a branch name or a commit SHA)
- (optionally) overridden values for the project's configuration (the settings in `dataform.json`)

A simple example of an `environments.json` file is:

```json
{
  "environments": [
    {
      "name": "staging",
      "gitReference": {
        "branch": "master"
      },
      "configOverride": {
        "defaultSchema": "dataform_staging",
        "assertionSchema": "dataform_assertions_staging"
      }
    }
  ]
}
```

This `staging` environment runs the code (and schedules) on the project's `master` branch. It also overrides the values of
`defaultSchema` and `assertionSchema` to isolate `staging` schedule runs from those in other environments.

Note that Dataform uses the `environments.json` file on your `master` branch to determine your project's environments.
**Any changes to your environments must be pushed to `master` before Dataform will take note of those changes**.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  If your project has a missing or empty <code>environments.json</code> file, Dataform uses a
  default environment which runs the code on your <code>master</code> branch with no project
  configuration overrides.
</div>

## Example staging to production using environments

In these paradigms, changes to the `production` environment are tightly controlled.

The concept is that a `production` environment holds the current stable version being used, while recent changes are stored in a `staging` environment. Once stable, `staging` can be merged in to `production` (which may require a pull request, depending on your project's settings).

This process has the nice property of requiring that any change to `production` is recorded in an audit trail (i.e. your project's Git commits).

The easiest way to differentiate staging and production data is by using different databases. This is done in the examples by overriding the project config. This is however **not valid in Redshift**; see [Recommended Redshift config overriding](#recommended-redshift-config-overriding).

### Configuration via commit SHA

In this configuration:

- the `production` environment is locked to a specific Git commitproduction data

- the `staging` environment runs the project's schedules at the latest version of the project's code (as exists on the `master` branch)

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
        "commitSha": -[Production commit sha here]-
      },
      "configOverride": {
        "defaultDatabase": "dataform_production"
      }
    }
  ]
}
```

To update the version of the project running in `production`, change the value of `commitSha`, and then push that change to your `master` branch. On GitHub the commit sha can be found by opening the project page, clicking commits, then copying the desired sha from the presented list of commits.

### Configuration via branches

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  Branches seen on dataform are not remote git branches, but are only used for local development. Because of this, a <code>branch</code> specified in a <code>gitReference</code> won't point a branch on dataform, with the exception of the master branch.
</div>

In this configuration:

- the `production` environment is locked to a specific remote git branch

- the `staging` environment runs the project's schedules at the latest version of the project's code (as exists on the `master` branch)

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
        "branch": "production"
      },
      "configOverride": {
        "defaultDatabase": "dataform_production"
      }
    }
  ]
}
```

To update the version of the project running in `production`, merge the `master` branch in to the `production` branch.

### Recommended Redshift config overriding

Redshift does not configure multiple databases like alternative providers. Overriding schemas instead of databases are a good solution to this. For example:

```json
{
  "environments": [
    {
      "name": "staging",
      "gitReference": {
        "branch": "master"
      },
      "configOverride": {
        "defaultSchema": "dataform_staging",
        "assertionSchema": "dataform_assertions_staging"
      }
    },
    {
      "name": "production",
      "gitReference": {
        "branch": "production"
      },
      "configOverride": {
        "defaultSchema": "dataform_prod",
        "assertionSchema": "dataform_assertions_prod"
      }
    }
  ]
}
```
