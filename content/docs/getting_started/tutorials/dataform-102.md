---
title: Dataform 102
priority: 1
---

## Create a new file

Create a new file by clicking on the + button next to the `definitions` folder and create a new SQLX file named `onboarding_table`.
For now, ignore the other options; these are more advanced features which will be covered in later documentation.

<img src="/static/images/how_to_guides/publish_tables/new_file.png" style="width: 100%;" />

## Write a query

In the text editor, write a simple SQLX statement:

```js
config { type: "view" }
SELECT 1 AS one,
       2 AS two,
       3 AS three
```

You will see the right sidebar update with `onboarding_table` which is the name of the dataset, a tag for the type - a `view` - as well as a validation message and two action buttons.

The `Preview results` button executes the query in your warehouse and returns the output at the bottom of the page. This can be useful during query development to check that the query returns expected output.

<img
src="/static/images/how_to_guides/publish_tables/compilation.png"
style="width: 100%"
/>

## Run the query

Clicking the `Run this action` button will open a dialog box with a few options for running your actions. Click `Run now` to continue.

Upon successful completion of the run, a new view named `dataform.onboarding_table` will be created in your warehouse.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" markdown="1">
  By default, Dataform creates datasets and views under a schema named <code>dataform</code>. You
  can change this in the <code>dataform.json</code> in your
  <a href="../guides/configuration">project configuration</a>.
</div>

<img src="/static/images/how_to_guides/publish_tables/run_node.png" style="width: 100%" />

## Create a table instead of a view

To create a table instead of a view, simply change your script's `config` block:

```js
config { type: "table" }
```

You will see the tag on the right sidebar update from `view` to `table`.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-none" markdown="1">
  Dataform provides many other options for dataset creation. Learn about{" "}
  <a href="../guides/datasets">publishing datasets</a> and{" "}
  <a href="../guides/incremental-datasets">incremental datasets</a>.
</div>

<img
src="/static/images/how_to_guides/publish_tables/table_vs_view.png"
style="width: 100%"
/>

## Referencing other datasets

Dataform provides methods that enable you to easily reference another dataset in your project using the `ref()` function:

```js
SELECT * FROM ${ref('my_table')}
```

This provides two advantages:

- You don’t have provide the full SQL dataset name.
- Any dataset that is referenced by a query will be automatically added to that query's dependencies. Dependency queries are always executed before dependent queries to ensure correctness.

## Create a new file

Create a new SQLX file named `onboarding_table2` and add the following content:

```js
config { type: "view" }
SELECT * FROM ${ref('onboarding_table')}
```

<img
src="/static/images/how_to_guides/publish_tables/ref_query.png"
style="width: 100%"
/>

You will see the right sidebar update with the name of the view, `onboarding_table2`. Clicking on `Compiled query` will display the fully qualified query to be run in your data warehouse.
The `${ref()}` function will be replaced with the actual name of the dataset in your data warehouse. Note that the dependency is also listed just above the compiled query.

_*Your compiled script will differ depending on your cloud data warehouse. This example uses Redshift.*_

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Referencing a dataset that hasn't yet been created in your data warehouse will cause validation
  errors. These validation errors are warning you that the dataset you are referencing doesn't exist
  (yet). However, since Dataform will run your project's scripts in dependency order, you can safely
  ignore these errors.
</div>

## View dependency tree

The overview page (linked at the top left of Dataform) gives you an overview of your project. The overview includes a visualization of the dependency tree of your project, containing the two datasets created by your scripts.

<img
src="/static/images/how_to_guides/publish_tables/dependencies.png"
style="width: 100%"
/>

In bigger projects containing dozens or hundreds of datasets with complicated interdependencies, the overview page can help you keep track of your project's overall structure.
