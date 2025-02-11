[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/actions/declaration"](../modules/_core_actions_declaration_.md) › [Declaration](_core_actions_declaration_.declaration.md)

# Class: Declaration

You can declare any BigQuery table as a data source in Dataform. Declaring BigQuery data
sources that are external to Dataform lets you treat those data sources as Dataform objects.

Declaring data sources is optional, but can be useful when you want to do the following:
* Reference or resolve declared sources in the same way as any other table in Dataform.
* View declared sources in the visualized Dataform graph.
* Use Dataform to manage the table-level and column-level descriptions of externally created
  tables.
* Trigger workflow invocations that include all the dependents of an external data source.

You can create declarations in the following ways. Available config options are defined in
[DeclarationConfig](configs#dataform-ActionConfig-DeclarationConfig), and are shared across all
the followiing ways of creating declarations.

**Using a SQLX file:**

```sql
-- definitions/name.sqlx
config {
  type: "declaration"
}
-- Note: no SQL should be present.
```

**Using action configs files:**

```yaml
# definitions/actions.yaml
actions:
- declare:
  name: name
```

**Using the Javascript API:**

```js
// definitions/file.js
declare("name")
```

## Hierarchy

* ActionBuilder‹Declaration›

  ↳ **Declaration**

## Index

### Methods

* [columns](_core_actions_declaration_.declaration.md#columns)
* [description](_core_actions_declaration_.declaration.md#description)

## Methods

###  columns

▸ **columns**(`columns`: ColumnDescriptor[]): *this*

*Defined in [core/actions/declaration.ts:124](https://github.com/dataform-co/dataform/blob/c51d616a/core/actions/declaration.ts#L124)*

**`deprecated`** Deprecated in favor of
[DeclarationConfig.columns](configs#dataform-ActionConfig-DeclarationConfig).

Sets the column descriptors of columns in this table.

**Parameters:**

Name | Type |
------ | ------ |
`columns` | ColumnDescriptor[] |

**Returns:** *this*

___

###  description

▸ **description**(`description`: string): *this*

*Defined in [core/actions/declaration.ts:110](https://github.com/dataform-co/dataform/blob/c51d616a/core/actions/declaration.ts#L110)*

**`deprecated`** Deprecated in favor of
[DeclarationConfig.description](configs#dataform-ActionConfig-DeclarationConfig).

Sets the description of this assertion.

**Parameters:**

Name | Type |
------ | ------ |
`description` | string |

**Returns:** *this*
