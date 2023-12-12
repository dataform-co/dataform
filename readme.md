# Dataform Core

Dataform core is an open source meta-language to create SQL tables and workflows in BigQuery. Dataform core extends SQL by providing a dependency management system, automated data quality testing, and data documentation.

Using Dataform core, data teams can build scalable SQL data transformation pipelines following software engineering best practices, like version control and testing.

![Data collections and integrations feed into Dataform, which exports this data to BI and analytics tools.](static/images/single-source-of-truth.png?raw=true)

## Data modeling with Dataform

- [Quickstart](https://cloud.google.com/dataform/docs/quickstart).
- [Create tables and views](https://cloud.google.com/dataform/docs/tables).
- [Configure dependencies](https://cloud.google.com/dataform/docs/define-table#define_table_structure_and_dependencies).
- Write [data quality checks](https://cloud.google.com/dataform/docs/assertions).
- Enable [scripting](https://cloud.google.com/dataform/docs/develop-workflows-js) and code re-use with a JavaScript API.
- Import [pre-defined packages](https://dataform-co.github.io/dataform/docs/packages), or create your own.

![A simple Dataform DAG illustrating table dependency](static/images/simple-dag.png?raw=true)

_Note: we have recently undergone a documentation transition from [docs.dataform.co](https://docs.dataform.co/) to [cloud.google.com/dataform/docs](https://cloud.google.com/dataform/docs). Content hosted on the old document site is published from the [`main_v1` branch](https://github.com/dataform-co/dataform/tree/main_v1)._

## Get started

### With the CLI

You can install the Dataform CLI tool using the following command line. Follow the [docs](https://cloud.google.com/dataform/docs/use-dataform-cli) to get started.

```
npm i -g @dataform/cli
```

### In Google Cloud Platform

Dataform in Google Cloud Platform provides a fully managed experience to build scalable data transformations pipelines in **BigQuery** using SQL. It includes:

- a cloud development environment to develop data assets with SQL and Dataform core and version control code with GitHub, GitLab, and other Git providers.
- a fully managed, serverless orchestration environment for data pipelines, fully integrated in Google Cloud Platform.

You can learn more on [cloud.google.com/dataform](https://cloud.google.com/dataform).

### How it works

- Read the [docs here](https://cloud.google.com/dataform/docs/overview).

## Example Projects

- [https://github.com/GoogleCloudPlatform/marketing-data-engine-dataform](https://github.com/GoogleCloudPlatform/marketing-data-engine-dataform).
- [https://github.com/wintermi/movielens-dataform](https://github.com/wintermi/movielens-dataform).
- [https://github.com/wintermi/bqe-dataform](https://github.com/wintermi/bqe-dataform).
- [https://github.com/wintermi/imdb-dataform](https://github.com/wintermi/imdb-dataform).
- [https://github.com/wintermi/fashion-dataform](https://github.com/wintermi/fashion-dataform).
- [https://github.com/G2H/dataform-stackoverflow](https://github.com/G2H/dataform-stackoverflow).
- [https://github.com/karcot1/dataform_deployment_sample](https://github.com/karcot1/dataform_deployment_sample).

## Want to report a bug or request a feature?

- For Dataform core / open source requests, you can open an [issue](https://github.com/dataform-co/dataform/issues) in GitHub.
- For Dataform in Google Cloud Platform, you can file a bug [here](https://issuetracker.google.com/issues/new?component=1193995&template=1698201), and file feature requests [here](https://issuetracker.google.com/issues/new?component=1193995&template=1713836).

## Want to contribute?

Check out our [contributors guide](https://github.com/dataform-co/dataform/blob/main/contributing.md) to get started with setting up the repo.
