[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/actions/test"](../modules/_core_actions_test_.md) › [Test](_core_actions_test_.test.md)

# Class: Test

Dataform test actions can be used to write unit tests for your generated SQL

You can create unit tests in the following ways.

**Using a SQLX file:**

```sql
-- definitions/name.sqlx
config {
  type: "test"
}

input "foo" {
  SELECT 1 AS bar
}

SELECT 1 AS bar
```

**Using the Javascript API:**

```js
// definitions/file.js
test("name")
  .input("sample_data", `SELECT 1 AS bar`)
  .expect(`SELECT 1 AS bar`);

publish("sample_data", { type: "table" }).query("SELECT 1 AS bar")
```

Note: When using the Javascript API, methods in this class can be accessed by the returned value.
This is where `input` and `expect` come from.

## Hierarchy

* ActionBuilder‹Test›

  ↳ **Test**

## Index

### Methods

* [dataset](_core_actions_test_.test.md#dataset)
* [expect](_core_actions_test_.test.md#expect)
* [input](_core_actions_test_.test.md#input)

## Methods

###  dataset

▸ **dataset**(`ref`: [Resolvable](../modules/_core_contextables_.md#resolvable)): *this*

Sets the schema (BigQuery dataset) in which to create the output of this action.

**Parameters:**

Name | Type |
------ | ------ |
`ref` | [Resolvable](../modules/_core_contextables_.md#resolvable) |

**Returns:** *this*

___

###  expect

▸ **expect**(`contextableQuery`: [Contextable](../modules/_core_contextables_.md#contextable)‹[IActionContext](../interfaces/_core_contextables_.iactioncontext.md), string›): *this*

Sets the expected output of the query to being tested against.

**Parameters:**

Name | Type |
------ | ------ |
`contextableQuery` | [Contextable](../modules/_core_contextables_.md#contextable)‹[IActionContext](../interfaces/_core_contextables_.iactioncontext.md), string› |

**Returns:** *this*

___

###  input

▸ **input**(`refName`: string | string[], `contextableQuery`: [Contextable](../modules/_core_contextables_.md#contextable)‹[IActionContext](../interfaces/_core_contextables_.iactioncontext.md), string›): *this*

Sets the input query to unit test against.

**Parameters:**

Name | Type |
------ | ------ |
`refName` | string &#124; string[] |
`contextableQuery` | [Contextable](../modules/_core_contextables_.md#contextable)‹[IActionContext](../interfaces/_core_contextables_.iactioncontext.md), string› |

**Returns:** *this*
