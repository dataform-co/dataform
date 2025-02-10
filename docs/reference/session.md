[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/session"](../modules/_core_session_.md) › [Session](_core_session_.session.md)

# Class: Session

Contains methods that are published globally, so can be invoked anywhere in the `/definitions`
folder of a Dataform project.

## Hierarchy

* **Session**

## Index

### Properties

* [projectConfig](_core_session_.session.md#projectconfig)

### Methods

* [assert](_core_session_.session.md#assert)
* [declare](_core_session_.session.md#declare)
* [notebook](_core_session_.session.md#notebook)
* [operate](_core_session_.session.md#operate)
* [publish](_core_session_.session.md#publish)
* [test](_core_session_.session.md#test)

## Properties

###  projectConfig

• **projectConfig**: *ProjectConfig*

*Defined in [core/session.ts:63](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/session.ts#L63)*

Stores the Dataform project configuration of the current Dataform project. Can be accessed via
the `dataform` global variable.

Example:

```js
dataform.projectConfig.vars.myVariableName === "myVariableValue"
```

## Methods

###  assert

▸ **assert**(`name`: string, `query?`: AContextable‹string›): *[Assertion](_core_actions_assertion_.assertion.md)*

*Defined in [core/session.ts:368](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/session.ts#L368)*

Adds a Dataform assertion the compiled graph.

Available only in the `/definitions` directory.

Example:
```js
// definitions/file.js

assert("name").query(ctx => "select 1");
```

<!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
publish. -->

**Parameters:**

Name | Type |
------ | ------ |
`name` | string |
`query?` | AContextable‹string› |

**Returns:** *[Assertion](_core_actions_assertion_.assertion.md)*

___

###  declare

▸ **declare**(`dataset`: ITarget): *Declaration*

*Defined in [core/session.ts:395](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/session.ts#L395)*

Declares the dataset as a Dataform data source.

Available only in the `/definitions` directory.

Example:
```js
// definitions/file.js

declare({name: "a-declaration"})
```

<!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
publish. -->

**Parameters:**

Name | Type |
------ | ------ |
`dataset` | ITarget |

**Returns:** *Declaration*

___

###  notebook

▸ **notebook**(`name`: string): *Notebook*

*Defined in [core/session.ts:446](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/session.ts#L446)*

Creates a Notebook action.

Available only in the `/definitions` directory.

Example:
```js
// definitions/file.js

notebook("notebook-name")
```

<!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
publish. -->
<!-- TODO(ekrekr): add tests for this method -->

**Parameters:**

Name | Type |
------ | ------ |
`name` | string |

**Returns:** *Notebook*

___

###  operate

▸ **operate**(`name`: string, `queries?`: [Contextable](../modules/_core_common_.md#contextable)‹[ICommonContext](../interfaces/_core_common_.icommoncontext.md), string | string[]›): *Operation*

*Defined in [core/session.ts:289](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/session.ts#L289)*

Defines a SQL operation.

Available only in the `/definitions` directory.

Example:

```js
// definitions/file.js

publish("published-table", {
  type: "table",
  dependencies: ["a-declaration"],
}).query(ctx => "SELECT 1 AS test");
```

**Parameters:**

Name | Type |
------ | ------ |
`name` | string |
`queries?` | [Contextable](../modules/_core_common_.md#contextable)‹[ICommonContext](../interfaces/_core_common_.icommoncontext.md), string &#124; string[]› |

**Returns:** *Operation*

___

###  publish

▸ **publish**(`name`: string, `queryOrConfig?`: [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string› | TableConfig | ViewConfig | IncrementalTableConfig | ILegacyTableConfig | any): *Table | IncrementalTable | View*

*Defined in [core/session.ts:318](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/session.ts#L318)*

Creates a table or view.

Available only in the `/definitions` directory.

Example:

```js
// definitions/file.js

operate("an-operation", ["SELECT 1", "SELECT 2"])
```

**Parameters:**

Name | Type |
------ | ------ |
`name` | string |
`queryOrConfig?` | [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string› &#124; TableConfig &#124; ViewConfig &#124; IncrementalTableConfig &#124; ILegacyTableConfig &#124; any |

**Returns:** *Table | IncrementalTable | View*

___

###  test

▸ **test**(`name`: string): *Test*

*Defined in [core/session.ts:420](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/session.ts#L420)*

Creates a Test action.

Available only in the `/definitions` directory.

Example:
```js
// definitions/file.js

test("test-name")
```

<!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
publish. -->
<!-- TODO(ekrekr): add tests for this method -->

**Parameters:**

Name | Type |
------ | ------ |
`name` | string |

**Returns:** *Test*
