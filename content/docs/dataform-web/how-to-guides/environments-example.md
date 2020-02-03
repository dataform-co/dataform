---
title: Environments example project
---

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
  Environments require <code>@dataform/core >= 1.4.9</code>.
</div>

## Introduction

This guide will show you how to configure a project with two environments. One environment will be staging, and the other will be production. Typical of this paradigm, staging will be upstream of production, allowing committed code to be tested before being released.

## Creating a project with environments

- Create an empty remote repository on a git provider, for example on [GitHub](https://github.com/new). The repository must be empty (don't create a README file). Alternatively if adding environments to a project that is already linked to a remote git provider, skip to adding the environments file.

- Go on the [dataform web app](https://app.dataform.co/).

  - Create a new project.

  - From settings, click `Migrate project to git provider`.

  - Follow the steps indicated, linking the empty remote repository to the project.

- Change to the development branch in the dataform app. Add the following files:

  - One of our template `sqlx` files, or anything that will show up in the run logs.

  - A schedule.

  - An environments file with the contents:

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

<div className="bp3-callout bp3-icon-info-sign bp3-intent-success" markdown="1">
This file descibes <b>replacing the production branch on dataform as a staging environment</b>, while using a <b>production branch in the remote repository as the actual production environment</b>. In order to update production from staging, the remote production branch can be merged in from the remote master branch. This is done at the discretion of the user, allowing for easy rollbacks in the case of issues.
</div>

- Commit the added files on dataform, and push them to master. These files and schedule will now be running in the staging environment, as described by the environments file.

  - The modelling overview page will now show the staging environment schedule.

  - The run logs will now indicate the environment as staging.

- In the remote repository, create a new branch called `production`.

- In the remote repository, merge master (the staging environment) in to the production branch (the production environment). Both production and staging will now be running the same files on the same schedule, as they are identical.

  - The modelling overview page will now show both the staging and production environment schedules.

  - The run logs will now indicate the environment as either staging or production, dependant on where the run was triggered from.
