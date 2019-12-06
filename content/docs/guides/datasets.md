---
title: Publishing datasets
---

<img src="/static/images/publishing_tables.png" />

## Create a view

Create a new `example.sqlx` file in your project under the `definitions/` folder:

```js
config { type: "view" }
SELECT 1 AS TEST
```

Upon running this query a **view** called `schema.example` will be created (where `schema` is the Dataform schema defined in your [`dataform.json`](configuration#dataform.json) file).

Note that non-SQL statements are stripped during query compilation, so the final executable query in this case would be:

```js
SELECT 1 AS TEST
```

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Trailing semi-colons should be omitted from queries.
</div>

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  File names must be unique within your <code>definitions/</code> folder, even across different
  directories, because these determine the name of the dataset or view created within your
  warehouse.
</div>

## Create a table

To create a table, i.e. a full independent copy of the query result, set the `type` of the query to `table`.

```js
config { type: "table" }
SELECT 1 AS TEST
```

## Referencing other datasets

Dataform provides methods that enable you to easily reference another dataset in your project without having to provide the full SQL dataset name.

`definitions/source.sql`:

```js
SELECT 1 AS sourcedata
```

`definitions/ref_example.sql`:

```js
SELECT * FROM ${ref("source")}
```

In order to reference the dataset created by a `source.sql` file, the value that should be passed to the `ref()` function is `"source"`, i.e. the name of the file defining the dataset (_without_ the file extension).

The query will be compiled into the following SQL before it is run:

```js
SELECT * FROM "dataform_schema"."source"
```

Any dataset that is referenced by a query will automatically be added to that query's dependencies. Dependency queries are always executed before dependent queries to ensure pipeline correctness.

## Adding custom dependencies

If you want to manually add a dependency to a query - one that is not already explicitly referenced with `ref()` - you should configure the file's `dependencies`:

```js
config { dependencies: [ "some_table" ] }
SELECT * FROM ...
```

Multiple dependencies may be provided in a single invocation:

```js
config { dependencies: [ "some_table", "some_other_table" ] }
SELECT * FROM ...
```

## Disable a dataset

To stop a query being run, you can disable it. This will keep the dataset as part of your graph, but stop it from executing when you run your project.
This can be useful for example if the relevant query breaks for some reason and you don't want your pipeline to fail while it's being fixed.

```js
config { disabled: true}
SELECT * FROM ...
```

## Executing statements before or after dataset creation

You can specify `pre_operations { ... }` and `post_operations { ... }` to configure Dataform to execute one or more SQL statements before or after creating a dataset.

### Example: Granting dataset access with `post_operations`

The following example defines a post-query operation to configure dataset access permissions. It makes use of the built-in `self()` method which returns the fully-qualified name of the current dataset.

```js
SELECT * FROM ...

post_operations {
  GRANT SELECT ON ${self()} TO GROUP "allusers@dataform.co"
}
```

To specify multiple operations, separate them with `---`:

```js
post_operations {
  GRANT SELECT ON ${self()} TO GROUP "allusers@dataform.co"
  ---
  GRANT SELECT ON ${self()} TO GROUP "otherusers@dataform.co"
}
```

## Overriding a dataset's schema or name

By default, a dataset's schema is set to the default schema chosen when initializing a project, usually `dataform`, and a dataset's name is set to the name of the corresponding file.
This can be overridden on a per-file basis by specifying the `schema` and/or `name` options. For example, to create a dataset called `example` in the schema `other_schema`:

```js
config {
  schema: "other_schema",
  name: "example"
}
SELECT * FROM ...
```

To reference datasets with overridden schemas, use the fully qualified name of the dataset:

```js
SELECT * FROM ${ref("example")}
```

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  If a dataset's <code>name</code> is overridden, you should use that name when calling the{" "}
  <code>ref()</code> function. For example, a dataset with{" "}
  <code>config &#123; name: "overridden_name" &#125;</code> would be referenced using{" "}
  <code>ref("overridden_name")</code>.
</div>

## Warehouse specific configuration

### BigQuery

For more information on configuring BigQuery datasets, such as enabling dataset partitioning, check out the [BigQuery guide](warehouses/bigquery).

### Redshift

For more information on configuring Redshift datasets, such as sort keys and dist keys, check out the [Redshift guide](warehouses/redshift).

### SQL Data Warehouse

For more information on configuring SQL Data Warehouse datasets, such as distribution settings, check out the [SQL Data Warehouse guide](warehouses/sqldatawarehouse).

## Further reading

- [Testing data with assertions](assertions)
- [Custom SQL operations](operations)
- [JavaScript API](js-api)
