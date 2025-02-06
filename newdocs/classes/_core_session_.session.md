[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/session"](../modules/_core_session_.md) › [Session](_core_session_.session.md)

# Class: Session

Contains methods that are published globally, so can be invoked anywhere in a Dataform project.

## Hierarchy

* **Session**

## Index

### Methods

* [publish](_core_session_.session.md#publish)

## Methods

###  publish

▸ **publish**(`name`: string, `queryOrConfig?`: Contextable‹ITableContext, string› | TableConfig | ViewConfig | IncrementalTableConfig | ILegacyTableConfig | any): *Table | IncrementalTable | View*

*Defined in [core/session.ts:283](https://github.com/dataform-co/dataform/blob/699b3c4c/core/session.ts#L283)*

Creates a table or view.

Available only in the /definitions directory.

**Parameters:**

Name | Type |
------ | ------ |
`name` | string |
`queryOrConfig?` | Contextable‹ITableContext, string› &#124; TableConfig &#124; ViewConfig &#124; IncrementalTableConfig &#124; ILegacyTableConfig &#124; any |

**Returns:** *Table | IncrementalTable | View*
