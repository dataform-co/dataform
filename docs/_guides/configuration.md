---
layout: documentation
title: Configuration
sub_headers: ["dataform.json", "package.json"]
---

Dataform projects are configured using two JSON files.

## `dataform.json`

Stores information about the project that is used to compile SQL, such as the warehouse type, default schemas, and warehouse specific settings.

See the [dataform.json reference](/reference/dataform-json) for more information.

## `package.json`

This is a regular Node/NPM package file which can be used it include JavaScript packages within your project.
