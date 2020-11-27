---
title: Configure CI/CD
subtitle: Configure continuous integration/deployment workflows for your Dataform project.
priority: 9
---

## Introduction

Continous Integration / Continuous Deployment (CI/CD) workflows help prevent code changes from unintentionally breaking your Dataform project.

Typically, these workflows are configured to run on every commit to your Git repository, automatically checking that the commit doesn't break your code.

## Dataform CLI Docker image

Dataform distributes [a Docker image](https://hub.docker.com/r/dataformco/dataform) which can be used to run the equivalent of [Dataform CLI](/dataform-cli) commands.

For most CI/CD tools, this Docker image is what you'll use to run your automated checks.

## Using GitHub Actions

If you host your Dataform Git repository on GitHub, you can use GitHub Actions to run CI/CD workflows. Read more about configuring workflows with GitHub Actions [here](https://help.github.com/en/actions/configuring-and-managing-workflows/configuring-a-workflow).

GitHub workflows are defined in YAML and must exist in a `.github/workflows` directory in your Git repository. Once the workflow file is added to this directory, GitHub will automatically run it on the events you specify in the workflow configuration.

For example, in a `.github/workflows/dataform.yaml` file:

```yaml
name: CI

on:
  push:
    branches:
      - master
  pull_request:
    branches:
      - master

jobs:
  compile:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code into workspace directory
        uses: actions/checkout@v2
      - name: Install project dependencies
        uses: docker://dataformco/dataform:latest
        with:
          args: install
      - name: Run dataform compile
        uses: docker://dataformco/dataform:latest
        with:
          args: compile
```

This workflow is configured to run on every commit to the `master` branch and for all pull requests to the `master` branch.

It contains three steps:

1. Check out the project code into the working directory
2. Use the Dataform Docker image to run `dataform install`, installing project dependencies into the working directory
3. Use the Dataform Docker image to run `dataform compile`, checking that the project still compiles correctly

If any of these steps fails, the workflow fails. The end result will be made visible in the GitHub UI (including logs to help you understand what, if anything, broke).

### Running commands which require warehouse credentials

You may want to run Dataform CLI commands that require credentials to access your data warehouse (for example, `dataform test` or `dataform run`). The Dataform CLI [expects](/dataform-cli#create-a-credentials-file) credentials to exist in a `.df-credentials.json` file. However, it would be insecure to commit that file to Git.

Fortunately, GitHub Actions have support for "secrets". A GitHub secret is configured in your GitHub project settings. Once configured, you can use this secret to decrypt your warehouse credentials as part of a GitHub Actions workflow.

1. Create your `.df-credentials.json` file by following [these steps](/dataform-cli#create-a-credentials-file)
2. Encrypt your credentials file: `gpg --symmetric --cipher-algo AES256 .df-credentials.json`, using a secret passphrase
3. Commit the encrypted `.df-credentials.json.gpg` file
4. Add the secret passphrase to your GitHub repository as a secret named `CREDENTIALS_GPG_PASSPHRASE`
5. Edit your GitHub Actions workflow file to decrypt the credentials file:

```yaml
name: CI

on:
  push:
    branches:
      - master
  pull_request:
    branches:
      - master

jobs:
  compile:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code into workspace directory
        uses: actions/checkout@v2
      - name: Install NPM dependencies
        uses: docker://dataformco/dataform:1.6.11
        with:
          args: install
      - name: Decrypt dataform credentials
        run: gpg --quiet --batch --yes --decrypt --passphrase="$CREDENTIALS_GPG_PASSPHRASE" --output .df-credentials.json .df-credentials.json.gpg
        env:
          CREDENTIALS_GPG_PASSPHRASE: ${{ secrets.CREDENTIALS_GPG_PASSPHRASE }}
      - name: Execute dataform run
        uses: docker://dataformco/dataform:1.6.11
        with:
          args: run
```

This workflow's final step runs `dataform run`, which uses the decrypted warehouse credentials file.

## Branch protection

CI/CD checks are intended to prevent project breakages before they happen. Therefore, we strongly recommend users to turn on [branch protection](https://help.github.com/en/github/administering-a-repository/about-protected-branches) for the `master` branch. Using branch protection, you can require that all changes to `master` are made through a pull request, and that those pull requests must pass the checks that you specify before they are mergeable.
