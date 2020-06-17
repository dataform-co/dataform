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

BigQuery and SQL Data Warehouse by default run all operations for a file in the same context by default. This functionality is not supported for Redshift or Snowflake.

The executed SQL is created by joining all operations with a semi-colon `;`. For example,

```sql
pre_operations {
  declare var string;
  set var = 'val';
}
select var as col;
```

is treated as if the `pre_operations` block wasn't there, becoming `declare var string; set var = 'val'; select var as col;`. However for Redshift or Snowflake, the pre_operations block would be executed as a separate query before the main block.

To disable running all operations in the same context, place the following flag in your `dataform.json` file:

```jsonkub
{
  ...
  "useSingleQueryPerAction": false,
  ...
}
```

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
