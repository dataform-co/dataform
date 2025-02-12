[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/actions/operation"](../modules/_core_actions_operation_.md) › [Operation](_core_actions_operation_.operation.md)

# Class: Operation

Operations define custom SQL operations that don't fit into the Dataform model of publishing a
table or writing an assertion.

You can create operations in the following ways. Available config options are defined in
[OperationConfig](configs#dataform-ActionConfig-OperationConfig), and are shared across all the
following ways of creating operations.

**Using a SQLX file:**

```sql
-- definitions/name.sqlx
config {
  type: "operations"
}
DELETE FROM dataset.table WHERE country = 'GB'
```

**Using action configs files:**

```yaml
# definitions/actions.yaml
actions:
- operation:
  filename: name.sql
```

```sql
-- definitions/name.sql
DELETE FROM dataset.table WHERE country = 'GB'
```

**Using the Javascript API:**

```js
// definitions/file.js
operate("name").query("DELETE FROM dataset.table WHERE country = 'GB'")
```

Note: When using the Javascript API, methods in this class can be accessed by the returned value.
This is where `query` comes from.

## Hierarchy

* ActionBuilder‹Operation›

  ↳ **Operation**

## Index

### Methods

* [columns](_core_actions_operation_.operation.md#columns)
* [database](_core_actions_operation_.operation.md#database)
* [dependencies](_core_actions_operation_.operation.md#dependencies)
* [description](_core_actions_operation_.operation.md#description)
* [disabled](_core_actions_operation_.operation.md#disabled)
* [hasOutput](_core_actions_operation_.operation.md#hasoutput)
* [hermetic](_core_actions_operation_.operation.md#hermetic)
* [queries](_core_actions_operation_.operation.md#queries)
* [schema](_core_actions_operation_.operation.md#schema)
* [setDependOnDependencyAssertions](_core_actions_operation_.operation.md#setdependondependencyassertions)
* [tags](_core_actions_operation_.operation.md#tags)

## Methods

###  columns

▸ **columns**(`columns`: ColumnDescriptor[]): *this*

**`deprecated`** Deprecated in favor of
[OperationConfig.columns](configs#dataform-ActionConfig-OperationConfig).

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
[OperationConfig.project](configs#dataform-ActionConfig-OperationConfig).

Sets the database (Google Cloud project ID) in which to create the corresponding view for this
operation.

**Parameters:**

Name | Type |
------ | ------ |
`database` | string |

**Returns:** *this*

___

###  dependencies

▸ **dependencies**(`value`: [Resolvable](../modules/_core_contextables_.md#resolvable) | [Resolvable](../modules/_core_contextables_.md#resolvable)[]): *this*

**`deprecated`** Deprecated in favor of
[OperationConfig.dependencies](configs#dataform-ActionConfig-OperationConfig).

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
[OperationConfig.description](configs#dataform-ActionConfig-OperationConfig).

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
[OperationConfig.disabled](configs#dataform-ActionConfig-OperationConfig).

If called with `true`, this action is not executed. The action can still be depended upon.
Useful for temporarily turning off broken actions.

**Parameters:**

Name | Type | Default |
------ | ------ | ------ |
`disabled` | boolean | true |

**Returns:** *this*

___

###  hasOutput

▸ **hasOutput**(`hasOutput`: boolean): *this*

**`deprecated`** Deprecated in favor of
[OperationConfig.hasOutput](configs#dataform-ActionConfig-OperationConfig).

Declares that this action creates a dataset which should be referenceable as a dependency
target, for example by using the `ref` function.

**Parameters:**

Name | Type |
------ | ------ |
`hasOutput` | boolean |

**Returns:** *this*

___

###  hermetic

▸ **hermetic**(`hermetic`: boolean): *void*

**`deprecated`** Deprecated in favor of
[OperationConfig.hermetic](configs#dataform-ActionConfig-OperationConfig).

If true, this indicates that the action only depends on data from explicitly-declared
dependencies. Otherwise if false, it indicates that the  action depends on data from a source
which has not been declared as a dependency.

**Parameters:**

Name | Type |
------ | ------ |
`hermetic` | boolean |

**Returns:** *void*

___

###  queries

▸ **queries**(`queries`: [Contextable](../modules/_core_contextables_.md#contextable)‹[IActionContext](../interfaces/_core_contextables_.iactioncontext.md), string | string[]›): *this*

Sets the query/queries to generate the operation from.

<!-- TODO(ekrekr): deprecated this in favor of a single `query(` method -->

**Parameters:**

Name | Type |
------ | ------ |
`queries` | [Contextable](../modules/_core_contextables_.md#contextable)‹[IActionContext](../interfaces/_core_contextables_.iactioncontext.md), string &#124; string[]› |

**Returns:** *this*

___

###  schema

▸ **schema**(`schema`: string): *this*

**`deprecated`** Deprecated in favor of
[OperationConfig.dataset](configs#dataform-ActionConfig-OperationConfig).

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
[OperationConfig.dependOnDependencyAssertions](configs#dataform-ActionConfig-OperationConfig).

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
[OperationConfig.tags](configs#dataform-ActionConfig-OperationConfig).

Sets a list of user-defined tags applied to this action.

**Parameters:**

Name | Type |
------ | ------ |
`value` | string &#124; string[] |

**Returns:** *this*
