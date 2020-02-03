---
title: Environments
---

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  Environments require <code>@dataform/core >= 1.4.9</code>.
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
Any changes to your environments must be pushed to `master` before Dataform will take note of those changes.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  If your project has a missing or empty <code>environments.json</code> file, Dataform uses a
  default environment which runs the code on your <code>master</code> branch with no project
  configuration overrides.
</div>

## Multiple environments

Once your team or project outgrows the default environment configuration, a typical configuration might look like the following:

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

This configuration defines two environments. The `staging` environment runs the project's schedules at the latest version of the
project's code (as exists on the `master` branch). When you have confirmed that the code works as expected, you can push those
schedules to production by merging the `master` branch into the `production` branch.

### Enforcing tighter control on changes to an environment

In some cases it may be preferred to tightly control changes to an environment. For example, you might want to enforce
that code is only pushed to a `production` environment after a pull request and/or code review.

```json
{
  "environments": [
    {
      "name": "production",
      "gitReference": {
        "commitSha": "67bed6bd4205ce97fa0284086ed70e5bc7f6dd75"
      }
    }
  ]
}
```

This example locks the `production` environment to a specific Git commit. To update the version of the project running in
`production`, you must change the value of `commitSha`, and then push that change to your `master` branch (which may
require a pull request, depending on your project's settings). This process has the nice property of requiring that any
change to `production` is recorded in an audit trail (i.e. your project's Git commits).
