<p align="center">
  <img src="https://github.com/dataform-co/dataform/blob/master/static/images/github_bg.png">
</p>

# Intro

Dataform core is an open source meta-language to create SQL tables and workflows. Dataform core extends SQL by providing a dependency management system, automated data quality testing, and data documentation.

Using Dataform core, data teams can build scalable SQL data transformation pipelines following software engineering best practices, like version control and testing.

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
* [Quickstart](https://cloud.google.com/dataform/docs/quickstart)
* [Create tables and views](https://cloud.google.com/dataform/docs/tables)
* [Configure dependencies](https://cloud.google.com/dataform/docs/define-table#define_table_structure_and_dependencies)
* Write [data quality checks](https://docs.dataform.co/guides/assertions)
* Enable [scripting](https://cloud.google.com/dataform/docs/develop-workflows-js) and code re-use with a JavaScript API

<div align="center">
  <img src="https://assets.dataform.co/docs/introduction/simple_dag.png" alt="Dependency tree in a Dataform project">
<i>Dependency tree in a Dataform project</i>
</div>


# Get started

## With the CLI

You can install the Dataform CLI tool using the following command line. Follow the [docs](https://cloud.google.com/dataform/docs/use-dataform-cli) to get started.


```
npm i -g @dataform/cli
```

<br/>

## In Google Cloud Platform

Dataform in Google Cloud Platform provides a fully managed experience to build scalable data transformations pipelines in **BigQuery** using SQL. It includes:
 - a cloud development environment to develop data assets with SQL and Dataform core and version control code with GitHub, GitLab, and other Git providers.
 - a fully managed, serverless orchestration environment for data pipelines, fully integrated in Google Cloud Platform.

You can learn more on [cloud.google.com/dataform](https://cloud.google.com/dataform)

## How it works
* Read the [docs here](https://cloud.google.com/dataform/docs/overview).


# Want to report a bug or request a feature?
* For Dataform core / open source requests, you can open an [issue](https://github.com/dataform-co/dataform/issues) in GitHub.
* For Dataform in Google Cloud Platform, you can file a bug [here](https://issuetracker.google.com/issues/new?component=1193995&template=1698201), and file feature requests [here](https://issuetracker.google.com/issues/new?component=1193995&template=1713836). 

# Want to contribute?
Check out our [contributors guide](https://github.com/dataform-co/dataform/blob/master/contributing.md) to get started with setting up the repo.
