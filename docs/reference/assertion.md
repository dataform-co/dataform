[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/actions/assertion"](../modules/_core_actions_assertion_.md) › [Assertion](_core_actions_assertion_.assertion.md)

# Class: Assertion

An assertion is a data quality test query that finds rows that violate one or more conditions
specified in the query. If the query returns any rows, the assertion fails.

You can create assertions in the following ways:

- Add built-in assertions to the config block of a table. @see [table](Table).
- Add manual assertions in a separate SQLX file. Example: `config { type: "assertion" }`.
- Use the Javascript API. Example: `assert("name")`.

When using the Javascript API, methods in this class can be accessed by the returned value. For
example, the query for an assertion can be set like this:

```
assert.query("SELECT * FROM table WHERE a IS NULL")
```

## Hierarchy

- ActionBuilder‹Assertion›

  ↳ **Assertion**

## Index

### Methods

- [database](_core_actions_assertion_.assertion.md#database)
- [dependencies](_core_actions_assertion_.assertion.md#dependencies)
- [description](_core_actions_assertion_.assertion.md#description)
- [disabled](_core_actions_assertion_.assertion.md#disabled)
- [hermetic](_core_actions_assertion_.assertion.md#hermetic)
- [query](_core_actions_assertion_.assertion.md#query)
- [schema](_core_actions_assertion_.assertion.md#schema)
- [tags](_core_actions_assertion_.assertion.md#tags)

## Methods

### database

▸ **database**(`database`: string): _this_

_Defined in [core/actions/assertion.ts:209](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/actions/assertion.ts#L209)_

**`deprecated`** Deprecated in favor of
[Project](dataform.ActionConfigs.AssertionConfig.Project).

Sets the database (Google Cloud project ID) in which to create the corresponding view for this
assertion.

**Parameters:**

| Name       | Type   |
| ---------- | ------ |
| `database` | string |

**Returns:** _this_

---

### dependencies

▸ **dependencies**(`value`: [Resolvable](../modules/_core_common_.md#resolvable) | [Resolvable](../modules/_core_common_.md#resolvable)[]): _this_

_Defined in [core/actions/assertion.ts:144](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/actions/assertion.ts#L144)_

**`deprecated`** Deprecated in favor of
[AssertionConfig.dependencies](configs#dataform-ActionConfig-AssertionConfig).

Sets dependencies of the assertion.

**Parameters:**

| Name    | Type                                                                                                               |
| ------- | ------------------------------------------------------------------------------------------------------------------ |
| `value` | [Resolvable](../modules/_core_common_.md#resolvable) &#124; [Resolvable](../modules/_core_common_.md#resolvable)[] |

**Returns:** _this_

---

### description

▸ **description**(`description`: string): _this_

_Defined in [core/actions/assertion.ts:197](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/actions/assertion.ts#L197)_

**`deprecated`** Deprecated in favor of
[Description](dataform.ActionConfigs.AssertionConfig.Description).

Sets the description of this assertion.

**Parameters:**

| Name          | Type   |
| ------------- | ------ |
| `description` | string |

**Returns:** _this_

---

### disabled

▸ **disabled**(`disabled`: boolean): _this_

_Defined in [core/actions/assertion.ts:171](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/actions/assertion.ts#L171)_

**`deprecated`** Deprecated in favor of [Disabled](dataform.ActionConfigs.AssertionConfig.Disabled).

If called with `true`, this action is not executed. The action can still be depended upon.
Useful for temporarily turning off broken actions.

**Parameters:**

| Name       | Type    | Default |
| ---------- | ------- | ------- |
| `disabled` | boolean | true    |

**Returns:** _this_

---

### hermetic

▸ **hermetic**(`hermetic`: boolean): _void_

_Defined in [core/actions/assertion.ts:159](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/actions/assertion.ts#L159)_

**`deprecated`** Deprecated in favor of [Hermetic](dataform.ActionConfigs.AssertionConfig.Hermetic).

Sets dependencies of the assertion.

**Parameters:**

| Name       | Type    |
| ---------- | ------- |
| `hermetic` | boolean |

**Returns:** _void_

---

### query

▸ **query**(`query`: AContextable‹string›): _this_

_Defined in [core/actions/assertion.ts:133](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/actions/assertion.ts#L133)_

Sets the query to be run by the assertion.

**Parameters:**

| Name    | Type                 |
| ------- | -------------------- |
| `query` | AContextable‹string› |

**Returns:** _this_

---

### schema

▸ **schema**(`schema`: string): _this_

_Defined in [core/actions/assertion.ts:227](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/actions/assertion.ts#L227)_

**`deprecated`** Deprecated in favor of
[Dataset](dataform.ActionConfigs.AssertionConfig.Dataset).

Sets the schema (BigQuery dataset) in which to create the corresponding view for this
assertion.

**Parameters:**

| Name     | Type   |
| -------- | ------ |
| `schema` | string |

**Returns:** _this_

---

### tags

▸ **tags**(`value`: string | string[]): _this_

_Defined in [core/actions/assertion.ts:181](https://github.com/dataform-co/dataform/blob/14a8a5d1/core/actions/assertion.ts#L181)_

**`deprecated`** Deprecated in favor of [Tags](dataform.ActionConfigs.AssertionConfig.Tags).

Sets a list of user-defined tags applied to this action.

**Parameters:**

| Name    | Type                   |
| ------- | ---------------------- |
| `value` | string &#124; string[] |

**Returns:** _this_
