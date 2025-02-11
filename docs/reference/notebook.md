[Dataform Javascript API Reference](../README.md) › [Globals](../globals.md) › ["core/actions/notebook"](../modules/_core_actions_notebook_.md) › [Notebook](_core_actions_notebook_.notebook.md)

# Class: Notebook

Notebooks run Jupyter Notebook files, and can output content to the storage buckets defined in
`workflow_settings.yaml` files.

You can create notebooks in the following ways. Available config options are defined in
[NotebookConfig](configs#dataform-ActionConfig-NotebookConfig), and are shared across all the
following ways of creating notebooks.

**Using action configs files:**

```yaml
# definitions/actions.yaml
actions:
- notebook:
  filename: name.ipynb
```

```ipynb
# definitions/name.ipynb
{ "cells": [] }
```

**Using the Javascript API:**

```js
// definitions/file.js
notebook("name", { filename: "name.ipynb" })
```

```ipynb
# definitions/name.ipynb
{ "cells": [] }
```

## Hierarchy

* ActionBuilder‹Notebook›

  ↳ **Notebook**

## Index

### Methods

* [ipynb](_core_actions_notebook_.notebook.md#ipynb)

## Methods

###  ipynb

▸ **ipynb**(`contents`: object): *[Notebook](_core_actions_notebook_.notebook.md)*

*Defined in [core/actions/notebook.ts:112](https://github.com/dataform-co/dataform/blob/1eef2cde/core/actions/notebook.ts#L112)*

Sets or overrides the contents of the notebook to run. Not recommended in general; using
separate `.ipynb` files for notebooks is preferred.

**Parameters:**

Name | Type |
------ | ------ |
`contents` | object |

**Returns:** *[Notebook](_core_actions_notebook_.notebook.md)*
