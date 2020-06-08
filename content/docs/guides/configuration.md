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

### Running pre and post operations in context

By default, pre and post operations aren't run in the same context as the main query. Because of this, variables declared in pre-operations won't be valid in the main query. Currently for BigQuery and SQL Data Warehouse there is the option to enable contextual pre and post operations by adding the following flag:

```json
{
  ...
  "useSingleQueryPerAction": true,
  ...
}
```

This will join pre-operations, the main query, and post operations with a `;`. In future, this functionality is likely to become the default.

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
