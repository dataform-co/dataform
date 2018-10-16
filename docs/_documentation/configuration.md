---
layout: documentation
title: Configuration
sub_headers: ["dataform.json", "package.json"]
---

# Configuration

## `dataform.json`

The `dataform.json` file stores project level information, such as the type of warehouse the project should compile against, or the default schemas/datasets to use.

An example `dataform.json` file:

```json
{
  "warehouse": "bigquery",
  "default_schema": "dataform_output",
  "assertion_schema": "dataform_tests",
}
```

### Warehouse

The `warehouse` setting specifies the type of warehouse the project will run against, and can be one of `["bigquery", "redshift", "snowflake", "postgres"]`.

### Default schema

The `default_schema` setting refers to the schema in your warehouse that materializations will be written to.

### Assertion schema

The `assertion_schema` setting refers to the schema in your warehouse that assertions will be written to. By default this are stored somewhere else to the normal materializations.

## `package.json`

This is a normal NPM package file. When adding compilation dependencies and packages to your project, they will appear here.
