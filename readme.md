<p align="center">
  <img src="https://github.com/dataform-co/dataform/blob/master/static/images/github_bg.png">
</p>
<p align="center">
  <a href="https://dataform.co/?utm_medium=organic&utm_source=github_readme">Dataform</a> is a tool for managing SQL based data operations in your warehouse.
</p>
<div align="center">
  <img src="https://storage.googleapis.com/dataform-cloud-build-badges/build/status.svg" alt="Cloud build status"/>
  <a href="https://www.npmjs.com/package/@dataform/cli"><img src="https://badge.fury.io/js/%40dataform%2Fcli.svg" alt="NPM package version" /></a>
  <a href="https://www.npmjs.com/package/@dataform/cli"><img alt="npm" src="https://img.shields.io/npm/dm/@dataform/cli.svg" alt="Monthly downloads" /></a>
</div>
<div align="center">
  <img src="https://david-dm.org/dataform-co/dataform.svg" alt="NPM dependency status" />
  <!-- <img src="https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A/badge.svg" alt="Dataform Slack" /> -->
  <img src="https://img.shields.io/github/license/dataform-co/dataform.svg" alt="License information" />
</div>
<div align="center">
  <!-- <a href="https://twitter.com/dataform"><img src="https://img.shields.io/twitter/follow/dataform.svg?style=social" alt="Follow Dataform on Twitter" /></a> -->
</div>

# Intro

[Dataform](https://dataform.co/?utm_medium=organic&utm_source=github_readme) is a platform to manage data in BigQuery, Snowflake, Redshift, and other data warehouses. It enables data teams to build scalable, tested, SQL based data transformation pipelines using version control and engineering inspired best practices. 

Compile hundreds of data models in under a second using SQLX. SQLX extends your existing SQL warehouse dialect to add features that support dependency management, testing, documentation and more.

<br/>
<br/>

<p align="center">
  <img src="https://assets.dataform.co/github-readme/single-source-of-truth%20(1).png">
</p>

### Supported warehouses
* BigQuery
* Snowflake
* Redshift
* Postgres
* Azure SQL data warehouse

# Data modeling with Dataform

* Turn any SQL query into a [dataset](https://docs.dataform.co/guides/datasets?utm_medium=organic&utm_source=github_readme) published back to your warehouse
* Write [data quality checks](https://docs.dataform.co/guides/assertions?utm_medium=organic&utm_source=github_readme) for your datasets
* Simplify generation of [incremental tables](https://docs.dataform.co/guides/incremental-datasets?utm_medium=organic&utm_source=github_readme) using merge/insert to save costs
* Generate a [DAG](https://docs.dataform.co/guides/datasets#referencing-other-datasets?utm_medium=organic&utm_source=github_readme) automatically from dataset dependencies
* [Document datasets](https://docs.dataform.co/guides/documentation?utm_medium=organic&utm_source=github_readme) in code alongside your SQL
* Enable [scripting](https://docs.dataform.co/guides/js-api?utm_medium=organic&utm_source=github_readme) and code re-use with a JavaScript API

<div align="center">
  <img src="https://assets.dataform.co/docs/introduction/simple_dag.png" alt="Dependency tree in a Dataform project">
<i>Dependency tree in a Dataform project</i>
</div>

### More examples and packages

* [Reading and writing data from S3](https://dataform.co/blog/import-data-s3-to-redshift?utm_medium=organic&utm_source=github_readme)
* [Writing unit tests](https://docs.dataform.co/guides/tests?utm_medium=organic&utm_source=github_readme)
* Create [slowly-changing dimension tables](https://github.com/dataform-co/dataform-scd?utm_medium=organic&utm_source=github_readme)
* Manage development, staging and production [environments](https://docs.dataform.co/dataform-web/guides/environments?utm_medium=organic&utm_source=github_readme)
* Model [Segment](https://dataform.co/blog/segment-package?utm_medium=organic&utm_source=github_readme) data in minutes
* Analyse [Bigquery](https://docs.dataform.co/packages/dataform-bq-audit-logs?utm_medium=organic&utm_source=github_readme) usage logs


# Get started

## With the CLI

You can install the Dataform SDK using the following command line. Follow the [docs](https://docs.dataform.co/guides/command-line-interface/?utm_medium=organic&utm_source=github_readme) to get started.


```
npm i -g @dataform/cli
```

<br/>

<img width="700" src="https://github.com/dataform-co/dataform/blob/master/static/images/gif.gif">

## With Dataform web

Dataform web is a development environment and production ready deployment tool for the Dataform SDK. You can learn more on [dataform.co](https://dataform.co/?utm_medium=organic&utm_source=github_readme?utm_medium=organic&utm_source=github_readme)

## How it works

- Read the [docs here](https://docs.dataform.co/getting-started#how-does-it-work?utm_medium=organic&utm_source=github_readme)

## More about Dataform
* [5 minute overview video](https://www.youtube.com/watch?v=axDKf0_FhYU&t=39s)
* Read about how we think you should approach [building a modern analytics stack](https://dataform.co/blog/modern-data-stack?utm_medium=organic&utm_source=github_readme)

# Join the Dataform community
* Join us on [Slack](https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A)
* Read our [blog](https://dataform.co/blog?utm_medium=organic&utm_source=github_readme)
* Check out what our [users say about us](https://dataform.co/customers?utm_medium=organic&utm_source=github_readme)

# Want to report a bug or request a feature?
* Create and upvote feature requests on [Canny](https://dataform.canny.io/admin/board/feature-requests)
* Message us on [Slack](https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A)
* Open an [issue](https://github.com/dataform-co/dataform/issues)

# Want to contribute?
Check out our [contributors guide](https://github.com/dataform-co/dataform/blob/master/contributing.md) to get started with setting up the repo.
