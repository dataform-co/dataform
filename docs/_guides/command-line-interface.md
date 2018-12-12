---
layout: documentation
title: Command line interface
sub_headers: ["Installation", "Create new project", "Project structure", "Define a materialization", "Compile project", "Dry run", "Create a profile", "Run project"]
---

## Installation

Dataform can be installed via Node:

```bash
npm i -g @dataform/cli
```

## Create new project

To create a new project in the folder `new_project`:

```bash
dataform init new_project --warehouse bigquery
```

Currently supported warehouse types are `[bigquery, redshift, snowflake, postgres]`

## Project structure

The default project structure is as follows:

<div class="bp3-tree bp3-elevation-0">
  <ul class="bp3-tree-node-list bp3-tree-root">
    <li class="bp3-tree-node bp3-tree-node-expanded">
      <div class="bp3-tree-node-content">
        <span class="bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard"></span>
        <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-folder-close"></span>
        <span class="bp3-tree-node-label">new_project</span>
      </div>
      <ul class="bp3-tree-node-list" style="margin-left: 20px">
        <li class="bp3-tree-node">
          <div class="bp3-tree-node-content">
            <span class="bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard"></span>
            <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-folder-close"></span>
            <span class="bp3-tree-node-label">definitions</span>
          </div>
        </li>
        <li class="bp3-tree-node">
          <div class="bp3-tree-node-content">
            <span class="bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard"></span>
            <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-folder-close"></span>
            <span class="bp3-tree-node-label">includes</span>
          </div>
        </li>
        <li class="bp3-tree-node">
          <div class="bp3-tree-node-content">
            <span class="bp3-tree-node-caret-none bp3-icon-standard"></span>
            <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-document"></span>
            <span class="bp3-tree-node-label">package.json</span>
          </div>
        </li>
        <li class="bp3-tree-node">
          <div class="bp3-tree-node-content">
            <span class="bp3-tree-node-caret-none bp3-icon-standard"></span>
            <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-document"></span>
            <span class="bp3-tree-node-label">dataform.json</span>
          </div>
        </li>
      </ul>
    </li>
  </ul>
</div>

## Define a materialization

The `definitions/` directory contains files that define [materializations](/materializations), [assertions](/assertions), and [operations](/operations).

To create a new materialization, create a new file such as `definitions/simplemodel.sql` with the following contents:

```sql
select 1 as test
```

## Compile project

To check everything has worked, run the following command at the root of your project directory to get JSON dump of the compiled project:

```bash
dataform compile
```

You should see output along the lines of:

```json
{
  "materializations": [
    {
      "name": "simplemodel",
      "type": "view",
      "target": {
        "schema": "dataform",
        "name": "simplemodel"
      },
      "query": "select 1 as test\n",
      "parsedColumns": [
        "test"
      ],
      "fileName": "definitions/simplemodel.sql"
    }
  ]
}
```

## Dry run

We can take this a step further and see exactly what statements and graph will be run, by executing the following:

```bash
dataform build
```

You should see something similar to the following:

```json
{
  "nodes": [
    {
      "name": "simplemodel",
      "tasks": [
        {
          "statement": "create or replace view `dataform.simplemodel`
           as select * from (select 1 as test)"
        }
      ]
    }
  ]
}
```

## Create a profile

A profile defines the connection parameters to your warehouse.

TODO

## Run project

If you are happy with the statements that will be executed, you can execute them against your data warehouse, by providing the path to your `profile.json` file created in the previous step.

```bash
dataform run --profile=<path-to-profile.json>
```
