[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/actions/view"](../modules/_core_actions_view_.md) › [View](_core_actions_view_.view.md)

# Class: View

Views are virtualised tables. They are useful for creating a new structured table without having
to copy the original data to it, which can result in significant cost savings for avoiding data
processing and storage.

You can create views in the following ways. Available config options are defined in
[ViewConfig](configs#dataform-ActionConfig-ViewConfig), and are shared across all the
following ways of creating tables.

**Using a SQLX file:**

```sql
-- definitions/name.sqlx
config {
  type: "view"
}
SELECT column FROM someTable
```

**Using action configs files:**

```yaml
# definitions/actions.yaml
actions:
- view:
  filename: name.sql
```

```sql
-- definitions/name.sql
SELECT column FROM someTable
```

**Using the Javascript API:**

```js
// definitions/file.js
table("name", { type: "view" }).query("SELECT column FROM someTable")
```

Note: When using the Javascript API, methods in this class can be accessed by the returned value.
This is where `query` comes from.

## Hierarchy

* ActionBuilder‹Table›

  ↳ **View**

## Index

### Methods

* [assertions](_core_actions_view_.view.md#assertions)
* [bigquery](_core_actions_view_.view.md#bigquery)
* [columns](_core_actions_view_.view.md#columns)
* [database](_core_actions_view_.view.md#database)
* [dependencies](_core_actions_view_.view.md#dependencies)
* [description](_core_actions_view_.view.md#description)
* [disabled](_core_actions_view_.view.md#disabled)
* [hermetic](_core_actions_view_.view.md#hermetic)
* [materialized](_core_actions_view_.view.md#materialized)
* [postOps](_core_actions_view_.view.md#postops)
* [preOps](_core_actions_view_.view.md#preops)
* [query](_core_actions_view_.view.md#query)
* [schema](_core_actions_view_.view.md#schema)
* [setDependOnDependencyAssertions](_core_actions_view_.view.md#setdependondependencyassertions)
* [tags](_core_actions_view_.view.md#tags)
* [type](_core_actions_view_.view.md#type)

## Methods

###  assertions

▸ **assertions**(`assertions`: TableAssertionsConfig): *this*

*Defined in [core/actions/view.ts:458](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L458)*

**`deprecated`** Deprecated in favor of
[ViewConfig.assertions](configs#dataform-ActionConfig-ViewConfig).

Sets in-line assertions for this view.

<!-- Note: this both applies in-line assertions, and acts as a method available via the JS API.
Usage of it via the JS API is deprecated, but the way it applies in-line assertions is still
needed -->

**Parameters:**

Name | Type |
------ | ------ |
`assertions` | TableAssertionsConfig |

**Returns:** *this*

___

###  bigquery

▸ **bigquery**(`bigquery`: IBigQueryOptions): *this*

*Defined in [core/actions/view.ts:328](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L328)*

**`deprecated`** Deprecated in favor of options available directly on
[ViewConfig](configs#dataform-ActionConfig-ViewConfig).

Sets bigquery options for the action.

**Parameters:**

Name | Type |
------ | ------ |
`bigquery` | IBigQueryOptions |

**Returns:** *this*

___

###  columns

▸ **columns**(`columns`: ColumnDescriptor[]): *this*

*Defined in [core/actions/view.ts:403](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L403)*

**`deprecated`** Deprecated in favor of
[ViewConfig.columns](configs#dataform-ActionConfig-ViewConfig).

Sets the column descriptors of columns in this view.

**Parameters:**

Name | Type |
------ | ------ |
`columns` | ColumnDescriptor[] |

**Returns:** *this*

___

###  database

▸ **database**(`database`: string): *this*

*Defined in [core/actions/view.ts:420](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L420)*

**`deprecated`** Deprecated in favor of
[ViewConfig.project](configs#dataform-ActionConfig-ViewConfig).

Sets the
Sets the database (Google Cloud project ID) in which to create the output of this action.

**Parameters:**

Name | Type |
------ | ------ |
`database` | string |

**Returns:** *this*

___

###  dependencies

▸ **dependencies**(`value`: [Resolvable](../modules/_core_common_.md#resolvable) | [Resolvable](../modules/_core_common_.md#resolvable)[]): *this*

*Defined in [core/actions/view.ts:345](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L345)*

**`deprecated`** Deprecated in favor of
[ViewConfig.dependencies](configs#dataform-ActionConfig-ViewConfig).

Sets dependencies of the view.

**Parameters:**

Name | Type |
------ | ------ |
`value` | [Resolvable](../modules/_core_common_.md#resolvable) &#124; [Resolvable](../modules/_core_common_.md#resolvable)[] |

**Returns:** *this*

___

###  description

▸ **description**(`description`: string): *this*

*Defined in [core/actions/view.ts:389](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L389)*

**`deprecated`** Deprecated in favor of
[ViewConfig.description](configs#dataform-ActionConfig-ViewConfig).

Sets the description of this view.

**Parameters:**

Name | Type |
------ | ------ |
`description` | string |

**Returns:** *this*

___

###  disabled

▸ **disabled**(`disabled`: boolean): *this*

*Defined in [core/actions/view.ts:304](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L304)*

**`deprecated`** Deprecated in favor of
[ViewConfig.disabled](configs#dataform-ActionConfig-ViewConfig).

If called with `true`, this action is not executed. The action can still be depended upon.
Useful for temporarily turning off broken actions.

**Parameters:**

Name | Type | Default |
------ | ------ | ------ |
`disabled` | boolean | true |

**Returns:** *this*

___

###  hermetic

▸ **hermetic**(`hermetic`: boolean): *void*

*Defined in [core/actions/view.ts:361](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L361)*

**`deprecated`** Deprecated in favor of
[ViewConfig.hermetic](configs#dataform-ActionConfig-ViewConfig).

If true, this indicates that the action only depends on data from explicitly-declared
dependencies. Otherwise if false, it indicates that the  action depends on data from a source
which has not been declared as a dependency.

**Parameters:**

Name | Type |
------ | ------ |
`hermetic` | boolean |

**Returns:** *void*

___

###  materialized

▸ **materialized**(`materialized`: boolean): *void*

*Defined in [core/actions/view.ts:318](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L318)*

**`deprecated`** Deprecated in favor of
[ViewConfig.materialized](configs#dataform-ActionConfig-ViewConfig).

Applies the materialized view optimization, see
https://cloud.google.com/bigquery/docs/materialized-views-intro.

**Parameters:**

Name | Type |
------ | ------ |
`materialized` | boolean |

**Returns:** *void*

___

###  postOps

▸ **postOps**(`posts`: [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string | string[]›): *this*

*Defined in [core/actions/view.ts:292](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L292)*

Sets a post-operation to run after the query is run. This is often used for revoking temporary
permissions granted to access source tables.

Example:

```js
// definitions/file.js
publish("example")
  .preOps(ctx => `GRANT \`roles/bigquery.dataViewer\` ON TABLE ${ctx.ref("other_table")} TO "group:automation@example.com"`)
  .query(ctx => `SELECT * FROM ${ctx.ref("other_table")}`)
  .postOps(ctx => `REVOKE \`roles/bigquery.dataViewer\` ON TABLE ${ctx.ref("other_table")} TO "group:automation@example.com"`)
```

**Parameters:**

Name | Type |
------ | ------ |
`posts` | [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string &#124; string[]› |

**Returns:** *this*

___

###  preOps

▸ **preOps**(`pres`: [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string | string[]›): *this*

*Defined in [core/actions/view.ts:273](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L273)*

Sets a pre-operation to run before the query is run. This is often used for temporarily
granting permission to access source tables.

Example:

```js
// definitions/file.js
publish("example")
  .preOps(ctx => `GRANT \`roles/bigquery.dataViewer\` ON TABLE ${ctx.ref("other_table")} TO "group:automation@example.com"`)
  .query(ctx => `SELECT * FROM ${ctx.ref("other_table")}`)
  .postOps(ctx => `REVOKE \`roles/bigquery.dataViewer\` ON TABLE ${ctx.ref("other_table")} TO "group:automation@example.com"`)
```

**Parameters:**

Name | Type |
------ | ------ |
`pres` | [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string &#124; string[]› |

**Returns:** *this*

___

###  query

▸ **query**(`query`: [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string›): *this*

*Defined in [core/actions/view.ts:248](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L248)*

Sets the query to generate the table from.

**Parameters:**

Name | Type |
------ | ------ |
`query` | [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string› |

**Returns:** *this*

___

###  schema

▸ **schema**(`schema`: string): *this*

*Defined in [core/actions/view.ts:437](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L437)*

**`deprecated`** Deprecated in favor of
[ViewConfig.dataset](configs#dataform-ActionConfig-ViewConfig).

Sets the schema (BigQuery dataset) in which to create the output of this action.

**Parameters:**

Name | Type |
------ | ------ |
`schema` | string |

**Returns:** *this*

___

###  setDependOnDependencyAssertions

▸ **setDependOnDependencyAssertions**(`dependOnDependencyAssertions`: boolean): *this*

*Defined in [core/actions/view.ts:522](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L522)*

**`deprecated`** Deprecated in favor of
[ViewConfig.dependOnDependencyAssertions](configs#dataform-ActionConfig-ViewConfig).

When called with `true`, assertions dependent upon any dependency will be add as dedpendency
to this action.

**Parameters:**

Name | Type |
------ | ------ |
`dependOnDependencyAssertions` | boolean |

**Returns:** *this*

___

###  tags

▸ **tags**(`value`: string | string[]): *this*

*Defined in [core/actions/view.ts:373](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L373)*

**`deprecated`** Deprecated in favor of
[ViewConfig.tags](configs#dataform-ActionConfig-ViewConfig).

Sets a list of user-defined tags applied to this action.

**Parameters:**

Name | Type |
------ | ------ |
`value` | string &#124; string[] |

**Returns:** *this*

___

###  type

▸ **type**(`type`: TableType): *this*

*Defined in [core/actions/view.ts:214](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/view.ts#L214)*

**`deprecated`** 
Deprecated in favor of action type can being set in the configs passed to action constructor
functions, for example `publish("name", { type: "table" })`.

**Parameters:**

Name | Type |
------ | ------ |
`type` | TableType |

**Returns:** *this*
