[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/actions/table"](../modules/_core_actions_table_.md) › [Table](_core_actions_table_.table.md)

# Class: Table

Tables are the fundamental building block for storing data when using Dataform. Dataform compiles
your Dataform core code into SQL, executes the SQL code, and creates your defined tables in
BigQuery.

You can create tables in the following ways. Available config options are defined in
[TableConfig](configs#dataform-ActionConfig-TableConfig), and are shared across all the
following ways of creating tables.

**Using a SQLX file:**

```sql
-- definitions/name.sqlx
config {
  type: "table"
}
SELECT 1
```

**Using action configs files:**

```yaml
# definitions/actions.yaml
actions:
- table:
  filename: name.sql
```

```sql
-- definitions/name.sql
SELECT 1
```

**Using the Javascript API:**

```js
// definitions/file.js
table("name", { type: "table" }).query("SELECT 1 AS TEST")
```

Note: When using the Javascript API, methods in this class can be accessed by the returned value.
This is where `query` comes from.

## Hierarchy

* ActionBuilder‹Table›

  ↳ **Table**

## Index

### Methods

* [assertions](_core_actions_table_.table.md#assertions)
* [bigquery](_core_actions_table_.table.md#bigquery)
* [columns](_core_actions_table_.table.md#columns)
* [database](_core_actions_table_.table.md#database)
* [dependencies](_core_actions_table_.table.md#dependencies)
* [description](_core_actions_table_.table.md#description)
* [disabled](_core_actions_table_.table.md#disabled)
* [hermetic](_core_actions_table_.table.md#hermetic)
* [postOps](_core_actions_table_.table.md#postops)
* [preOps](_core_actions_table_.table.md#preops)
* [query](_core_actions_table_.table.md#query)
* [schema](_core_actions_table_.table.md#schema)
* [setDependOnDependencyAssertions](_core_actions_table_.table.md#setdependondependencyassertions)
* [tags](_core_actions_table_.table.md#tags)
* [type](_core_actions_table_.table.md#type)

## Methods

###  assertions

▸ **assertions**(`assertions`: TableAssertionsConfig): *this*

**`deprecated`** Deprecated in favor of
[TableConfig.assertions](configs#dataform-ActionConfig-TableConfig).

Sets in-line assertions for this table.

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

**`deprecated`** Deprecated in favor of options available directly on
[TableConfig](configs#dataform-ActionConfig-TableConfig). For example:
`publish("name", { type: "table", partitionBy: "column" }`).

Sets bigquery options for the action.

**Parameters:**

Name | Type |
------ | ------ |
`bigquery` | IBigQueryOptions |

**Returns:** *this*

___

###  columns

▸ **columns**(`columns`: ColumnDescriptor[]): *this*

**`deprecated`** Deprecated in favor of
[TableConfig.columns](configs#dataform-ActionConfig-TableConfig).

Sets the column descriptors of columns in this table.

**Parameters:**

Name | Type |
------ | ------ |
`columns` | ColumnDescriptor[] |

**Returns:** *this*

___

###  database

▸ **database**(`database`: string): *this*

**`deprecated`** Deprecated in favor of
[TableConfig.project](configs#dataform-ActionConfig-TableConfig).

Sets the database (Google Cloud project ID) in which to create the output of this action.

**Parameters:**

Name | Type |
------ | ------ |
`database` | string |

**Returns:** *this*

___

###  dependencies

▸ **dependencies**(`value`: [Resolvable](../modules/_core_contextables_.md#resolvable) | [Resolvable](../modules/_core_contextables_.md#resolvable)[]): *this*

**`deprecated`** Deprecated in favor of
[TableConfig.dependencies](configs#dataform-ActionConfig-TableConfig).

Sets dependencies of the table.

**Parameters:**

Name | Type |
------ | ------ |
`value` | [Resolvable](../modules/_core_contextables_.md#resolvable) &#124; [Resolvable](../modules/_core_contextables_.md#resolvable)[] |

**Returns:** *this*

___

###  description

▸ **description**(`description`: string): *this*

**`deprecated`** Deprecated in favor of
[TableConfig.description](configs#dataform-ActionConfig-TableConfig).

Sets the description of this assertion.

**Parameters:**

Name | Type |
------ | ------ |
`description` | string |

**Returns:** *this*

___

###  disabled

▸ **disabled**(`disabled`: boolean): *this*

**`deprecated`** Deprecated in favor of
[TableConfig.disabled](configs#dataform-ActionConfig-TableConfig).

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

**`deprecated`** Deprecated in favor of
[TableConfig.hermetic](configs#dataform-ActionConfig-TableConfig).

If true, this indicates that the action only depends on data from explicitly-declared
dependencies. Otherwise if false, it indicates that the  action depends on data from a source
which has not been declared as a dependency.

**Parameters:**

Name | Type |
------ | ------ |
`hermetic` | boolean |

**Returns:** *void*

___

###  postOps

▸ **postOps**(`posts`: [Contextable](../modules/_core_contextables_.md#contextable)‹[ITableContext](../interfaces/_core_contextables_.itablecontext.md), string | string[]›): *this*

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
`posts` | [Contextable](../modules/_core_contextables_.md#contextable)‹[ITableContext](../interfaces/_core_contextables_.itablecontext.md), string &#124; string[]› |

**Returns:** *this*

___

###  preOps

▸ **preOps**(`pres`: [Contextable](../modules/_core_contextables_.md#contextable)‹[ITableContext](../interfaces/_core_contextables_.itablecontext.md), string | string[]›): *this*

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
`pres` | [Contextable](../modules/_core_contextables_.md#contextable)‹[ITableContext](../interfaces/_core_contextables_.itablecontext.md), string &#124; string[]› |

**Returns:** *this*

___

###  query

▸ **query**(`query`: [Contextable](../modules/_core_contextables_.md#contextable)‹[ITableContext](../interfaces/_core_contextables_.itablecontext.md), string›): *this*

Sets the query to generate the table from.

**Parameters:**

Name | Type |
------ | ------ |
`query` | [Contextable](../modules/_core_contextables_.md#contextable)‹[ITableContext](../interfaces/_core_contextables_.itablecontext.md), string› |

**Returns:** *this*

___

###  schema

▸ **schema**(`schema`: string): *this*

**`deprecated`** Deprecated in favor of
[TableConfig.dataset](configs#dataform-ActionConfig-TableConfig).

Sets the schema (BigQuery dataset) in which to create the output of this action.

**Parameters:**

Name | Type |
------ | ------ |
`schema` | string |

**Returns:** *this*

___

###  setDependOnDependencyAssertions

▸ **setDependOnDependencyAssertions**(`dependOnDependencyAssertions`: boolean): *this*

**`deprecated`** Deprecated in favor of
[TableConfig.dependOnDependencyAssertions](configs#dataform-ActionConfig-TableConfig).

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

**`deprecated`** Deprecated in favor of
[TableConfig.tags](configs#dataform-ActionConfig-TableConfig).

Sets a list of user-defined tags applied to this action.

**Parameters:**

Name | Type |
------ | ------ |
`value` | string &#124; string[] |

**Returns:** *this*

___

###  type

▸ **type**(`type`: TableType): *this*

**`deprecated`** 
Deprecated in favor of action type can being set in the configs passed to action constructor
functions, for example `publish("name", { type: "table" })`.

**Parameters:**

Name | Type |
------ | ------ |
`type` | TableType |

**Returns:** *this*
