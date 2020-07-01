---
title: Configure your project
subtitle: Learn how to configure your Dataform project.
priority: 5
---

## Introduction

A Dataform project is primarily configured through the `dataform.json` file that is created at the top level of your project directory.

In addition, `package.json` is used to control NPM dependency versions, including the current Dataform version.

## dataform.json

This file contains information about the project. These settings, such as the warehouse type, default schema names, and so on, are used to compile final SQL.

The following is an example of the `dataform.json` file for a BigQuery project:

```json
{
  "warehouse": "bigquery",
  "defaultDatabase": "my-gcp-project-id",
  "defaultSchema": "dataform",
  "assertionsSchema": "dataform_assertions"
}
```

### Changing default schema names

Dataform aims to create all objects under a single schema (or dataset in BigQuery) in your warehouse. This is usually called `dataform` but can be changed
by changing the `defaultSchema` property to some other value. For example, to change it to `mytables`, update the configuration file as following:

```json
{
  ...
  "defaultSchema": "mytables",
  ...
}
```

[Assertions](assertions) are created inside a different schema as specified by the `assertionsSchema` property.

### Running operations in context

BigQuery and SQL Data Warehouse by default run all operations for a file in the same context. This functionality is not supported for Redshift or Snowflake.

This is useful for scripting, for example defining variables or UDFs in BigQuery before the create table statement.

The executed SQL is created by joining all operations with a semi-colon `;`. For example,

```sql
pre_operations {
  declare var string;
  set var = 'val';
}
select var as col;
```

is treated as if the `pre_operations` block wasn't there, becoming `declare var string; set var = 'val'; select var as col;`. For a table, SQL would be injected into the relevant place; the previous statement would be executed as `` declare var string; set var = 'val'; create or replace table `tada-analytics.dataform_data.silly_table` as select var as col; ``.

For Redshift or Snowflake, the `pre_operations` block is always executed as a separate query before the main block.

To disable running all operations in the same context, place the following flag in your `dataform.json` file:

```json
{
  ...
  "useSingleQueryPerAction": false,
  ...
}
```

### Enable run caching to cut warehouse costs

Dataform has a built-in run caching feature. Once enabled, Dataform only runs actions (datasets, assertions, or operations) that might update the data in the action's output.

For example, if a dataset's SQL definition and dependency datasets are unchanged (since the previous run), re-creating that dataset will not update the actual data. In this case, with run caching enabled, Dataform would not run the relevant action.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Run caching depends on Dataform having accurate information about your project's dependency graph. All dependencies must be <a href="/guides/datasets/publish#referencing-other-datasets">declared</a> explicitly with <code>ref()</code> or <code>dependencies</code>.
</div>

To enable run caching on your project, add the following flag to your `dataform.json` file:

```json
{
  ...
  "useRunCache": true,
  ...
}
```

Run caching enforces some tighter compilation checks on your project. Actions with zero dependencies must either be changed to depend on [declarations](declarations), or must explicitly declare whether or not they are hermetic, using the `hermetic` [configuration option](../reference#ITableConfig).

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Any actions which depends on data from a source that has not been explicitly declared as a dependency should be explicitly marked as not hermetic, by setting <code>hermetic: false</code> on that action. This notifies Dataform that the action reads data from an undeclared dependency, and thus the action should always run.
</div>

## package.json

This is a standard NPM package file which may be used to include JavaScript packages within your project.

Most importantly, your Dataform version is specified here, and can be updated by changing this file or running the `npm install` or `npm update` commands inside your project directory.

If you develop projects on <a target="_blank" rel="noopener" href="https://dataform.co">Dataform Web</a>, this is managed for you and can be largely ignored.

### Updating Dataform to the latest version

All Dataform projects depend on the `@dataform/core` NPM package. If you are developing your project locally and would like to upgrade your Dataform version, run the following command:

```bash
npm update @dataform/core
```

If you use the `dataform` command line tool, you may also wish to upgrade your globally installed Dataform version:

```bash
npm update -g @dataform/cli
```
