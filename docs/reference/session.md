[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/session"](../modules/_core_session_.md) › [Session](_core_session_.session.md)

# Class: Session

Contains methods that are published globally, so can be invoked anywhere in the `/definitions`
folder of a Dataform project.

## Hierarchy

- **Session**

## Index

### Properties

- [projectConfig](_core_session_.session.md#projectconfig)

### Methods

- [assert](_core_session_.session.md#assert)
- [declare](_core_session_.session.md#declare)
- [notebook](_core_session_.session.md#notebook)
- [operate](_core_session_.session.md#operate)
- [publish](_core_session_.session.md#publish)
- [test](_core_session_.session.md#test)

## Properties

### projectConfig

• **projectConfig**: _ProjectConfig_

Stores the Dataform project configuration of the current Dataform project. Can be accessed via
the `dataform` global variable.

Example:

```js
dataform.projectConfig.vars.myVariableName === "myVariableValue";
```

## Methods

### assert

▸ **assert**(`name`: string, `query?`: AContextable‹string›): _[Assertion](_core_actions_assertion_.assertion.md)_

Adds a Dataform assertion the compiled graph.

Available only in the `/definitions` directory.

**`see`** [assertion](Assertion) for examples on how to use.

**Parameters:**

| Name     | Type                 |
| -------- | -------------------- |
| `name`   | string               |
| `query?` | AContextable‹string› |

**Returns:** _[Assertion](_core_actions_assertion_.assertion.md)_

---

### declare

▸ **declare**(`dataset`: ITarget): _[Declaration](_core_actions_declaration_.declaration.md)_

Declares the dataset as a Dataform data source.

Available only in the `/definitions` directory.

**`see`** [Declaration](Declaration) for examples on how to use.

<!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
publish. -->

**Parameters:**

| Name      | Type    |
| --------- | ------- |
| `dataset` | ITarget |

**Returns:** _[Declaration](_core_actions_declaration_.declaration.md)_

---

### notebook

▸ **notebook**(`name`: string): _[Notebook](_core_actions_notebook_.notebook.md)_

Creates a Notebook action.

Available only in the `/definitions` directory.

**`see`** [Notebook](Notebook) for examples on how to use.

<!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
publish. -->
<!-- TODO(ekrekr): add tests for this method -->

**Parameters:**

| Name   | Type   |
| ------ | ------ |
| `name` | string |

**Returns:** _[Notebook](_core_actions_notebook_.notebook.md)_

---

### operate

▸ **operate**(`name`: string, `queries?`: [Contextable](../modules/_core_common_.md#contextable)‹[IActionContext](../interfaces/_core_common_.IActionContext.md), string | string[]›): _[Operation](_core_actions_operation_.operation.md)_

Defines a SQL operation.

Available only in the `/definitions` directory.

**`see`** [operation](Operation) for examples on how to use.

**Parameters:**

| Name       | Type                                                                                                                                            |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`     | string                                                                                                                                          |
| `queries?` | [Contextable](../modules/_core_common_.md#contextable)‹[IActionContext](../interfaces/_core_common_.IActionContext.md), string &#124; string[]› |

**Returns:** _[Operation](_core_actions_operation_.operation.md)_

---

### publish

▸ **publish**(`name`: string, `queryOrConfig?`: [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string› | TableConfig | ViewConfig | IncrementalTableConfig | ILegacyTableConfig | any): _[Table](_core_actions_table_.table.md) | [IncrementalTable](_core_actions_incremental_table_.incrementaltable.md) | [View](_core_actions_view_.view.md)_

Creates a table, view, or incremental table.

Available only in the `/definitions` directory.

**`see`** [Operation](Operation) for examples on how to make tables.

**`see`** [View](View) for examples on how to make views.

**`see`** [IncrementalTable](IncrementalTable) for examples on how to make incremental tables.

**Parameters:**

| Name             | Type                                                                                                                                                                                                                                         |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`           | string                                                                                                                                                                                                                                       |
| `queryOrConfig?` | [Contextable](../modules/_core_common_.md#contextable)‹[ITableContext](../interfaces/_core_actions_index_.itablecontext.md), string› &#124; TableConfig &#124; ViewConfig &#124; IncrementalTableConfig &#124; ILegacyTableConfig &#124; any |

**Returns:** _[Table](_core_actions_table_.table.md) | [IncrementalTable](_core_actions_incremental_table_.incrementaltable.md) | [View](_core_actions_view_.view.md)_

---

### test

▸ **test**(`name`: string): _[Test](_core_actions_test_.test.md)_

Creates a Test action.

Available only in the `/definitions` directory.

**`see`** [Test](Test) for examples on how to use.

<!-- TODO(ekrekr): safely allow passing of config blocks as the second argument, similar to
publish. -->
<!-- TODO(ekrekr): add tests for this method -->

**Parameters:**

| Name   | Type   |
| ------ | ------ |
| `name` | string |

**Returns:** _[Test](_core_actions_test_.test.md)_
