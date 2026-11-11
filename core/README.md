# Dataform Core (`@dataform/core`)

`@dataform/core` is the open-source SQLX compiler, JavaScript workflow SDK, and Protocol Buffer schema package for [Dataform](https://cloud.google.com/dataform/docs/overview). It compiles SQLX (`.sqlx`), JavaScript (`.js`), action configuration (`actions.yaml`), BigQuery Property Graph (`graph.yaml`), and Jupyter Notebook (`.ipynb`) definitions into a dependency-ordered execution graph (`dataform.CompiledGraph`) for BigQuery.

For command-line execution (`dataform init`, `compile`, `test`, `run`, `format`), see the [Dataform CLI Reference](https://github.com/dataform-co/dataform/blob/main/cli/README.md) (`@dataform/cli`) and the [root repository overview](https://github.com/dataform-co/dataform/blob/main/readme.md).

---

## Contents

- [Installation and Version Pinning](#installation-and-version-pinning)
- [Project Structure](#project-structure)
- [Project Configuration (`workflow_settings.yaml`)](#project-configuration-workflow_settingsyaml)
- [SQLX Syntax Reference](#sqlx-syntax-reference)
  - [SQLX Code Blocks](#sqlx-code-blocks)
  - [SQLX Context Functions](#sqlx-context-functions)
- [Action Types Reference](#action-types-reference)
  - [`table`](#table)
  - [`incremental`](#incremental)
  - [`view`](#view)
  - [`operations`](#operations)
  - [`assertion` and Built-in Assertions](#assertion-and-built-in-assertions)
  - [`declaration`](#declaration)
  - [`test` (Unit Tests)](#test-unit-tests)
  - [`notebook`](#notebook)
  - [`propertyGraph` (`graph.yaml`)](#propertygraph-graphyaml)
  - [`actions.yaml` Action Configs](#actionsyaml-action-configs)
- [JavaScript API (`Session` Globals)](#javascript-api-session-globals)
- [Package Exports and Bundled `.proto` Files](#package-exports-and-bundled-proto-files)
- [Documentation and Links](#documentation-and-links)

---

## Installation and Version Pinning

### Via `workflow_settings.yaml` (Recommended)

In both **Dataform in Google Cloud** and **`@dataform/cli`**, specify `dataformCoreVersion` in your project's `workflow_settings.yaml`:

```yaml
defaultProject: my-gcp-project
defaultLocation: US
defaultDataset: dataform
defaultAssertionDataset: dataform_assertions
dataformCoreVersion: 3.1.5
```

When `dataformCoreVersion` is set in `workflow_settings.yaml`, `@dataform/cli` automatically installs that version of `@dataform/core` in an isolated temporary directory at compile time (see [Stateless installs](https://github.com/dataform-co/dataform/blob/main/docs/stateless-install.md)).

### Via `package.json` / `npm`

If you manage `@dataform/core` and external JavaScript packages with `package.json` (omitting `dataformCoreVersion` from `workflow_settings.yaml`), or if you consume `@dataform/core` programmatically:

```bash
npm i @dataform/core
```

---

## Project Structure

A standard Dataform project has the following layout:

```text
my-dataform-project/
├── workflow_settings.yaml     # Project-level settings and defaults
├── definitions/               # Action definitions (.sqlx, .js, actions.yaml, graph.yaml, .sql, .ipynb)
│   ├── sources.sqlx
│   ├── staging/
│   │   └── stg_orders.sqlx
│   └── marts/
│       └── fct_orders.sqlx
└── includes/                  # Shared JavaScript modules and constants
    └── constants.js
```

- **`workflow_settings.yaml`**: Configures default project/dataset/location, custom variables (`vars`), target prefixes/suffixes, and runtime options.
- **`definitions/`**: Every `.sqlx` and `.js` file under `definitions/` (and any subdirectory) is evaluated to register actions in the compilation session. `definitions/**/actions.yaml` files register YAML-configured actions, and `definitions/**/graph.yaml` defines a BigQuery Property Graph.
- **`includes/`**: Top-level `includes/*.js` files are automatically attached to the global scope by their file basename (for example, `includes/constants.js` is available as `constants` inside any `.sqlx` or `.js` file in `definitions/`). Nested files under `includes/` can be imported via standard `require("includes/...")`.

> **Note on Project Execution & Security:** Dataform compiles SQLX and JavaScript project code using Node.js's native `node:vm` context. While module resolution is scoped to the project directory, `node:vm` is not a security sandbox against untrusted code. Only compile and run Dataform projects from trusted sources.

---

## Project Configuration (`workflow_settings.yaml`)

`workflow_settings.yaml` is validated against the [`dataform.WorkflowSettings`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-WorkflowSettings) schema.

| Field | Type | Default | Description |
|---|---|---|---|
| `dataformCoreVersion` | `string` | — | `@dataform/core` version to compile against (for example, `3.1.5`). |
| `defaultProject` | `string` | — | Default Google Cloud project ID (`database`) for compiled actions. |
| `defaultDataset` | `string` | `"dataform"` | Default BigQuery dataset (`schema`) for tables, views, and operations. |
| `defaultLocation` | `string` | — | Default BigQuery dataset location (for example, `US`, `EU`, `us-central1`). |
| `defaultAssertionDataset` | `string` | `"dataform_assertions"` | Default BigQuery dataset (`schema`) for assertion views. |
| `vars` | `map<string, string>` | `{}` | Custom string key-value variables accessible via `dataform.projectConfig.vars.<key>`. |
| `projectSuffix` | `string` | — | Suffix appended to target Google Cloud project IDs (`<project>_<projectSuffix>`). |
| `datasetSuffix` | `string` | — | Suffix appended to target dataset names (`<dataset>_<datasetSuffix>`). |
| `namePrefix` | `string` | — | Prefix prepended to target action names (`<namePrefix>_<name>`). |
| `builtinAssertionNamePrefix` | `string` | — | Prefix prepended to auto-generated built-in assertion names. |
| `disableAssertions` | `boolean` | `false` | Marks all manual and built-in assertions as `disabled: true` in the compiled graph. |
| `defaultReservation` | `string` | — | Default BigQuery reservation for query execution (`projects/<p>/locations/<l>/reservations/<r>` or `none`). |
| `defaultJobLabels` | `map<string, string>` | `{}` | Default labels applied to BigQuery jobs executed for actions in the project. |
| `preserveGovernanceControls` | `boolean` | `false` | Preserves existing BigQuery column governance controls (such as Data Policies) when recreating tables. |
| `lineage` | `object` | — | Knowledge Catalog Data Lineage settings (`lineage: { enabled: true }`). See [Data Lineage guide](https://github.com/dataform-co/dataform/blob/main/docs/lineage.md). |
| `defaultIcebergConfig` | `object` | — | Project-level defaults for BigQuery Apache Iceberg tables (`bucketName`, `tableFolderRoot`, `tableFolderSubpath`, `connection`). |
| `defaultLakehouseConfig` | `object` | — | Project-level defaults for 4-part Lakehouse targets (`catalog`, `namespace`). |
| `defaultNotebookRuntimeOptions` | `object` | — | Default runtime options for Notebook actions (`outputBucket`, `runtimeTemplateName`, `repositorySnapshotDestination`). |
| `includeTestsInCompiledGraph` | `boolean` | `false` | Includes unit test actions (`type: "test"`) in `CompiledGraph.tests`. |
| `extension` | `object` | — | External compilation extension package configuration (`name`, `compilationMode`). |

---

## SQLX Syntax Reference

A `.sqlx` file combines a `config {}` metadata block, optional JavaScript (`js {}`) and hook blocks, and a SQL body with `${...}` JavaScript template interpolation.

### SQLX Code Blocks

| Block | Supported Action Types | Description |
|---|---|---|
| `config { ... }` | All `.sqlx` types | Declares the action `type` (defaults to `"operations"` if omitted), target overrides, dependencies, assertions, and BigQuery options. |
| `js { ... }` | All `.sqlx` types | Local JavaScript variables and helper functions scoped to the current `.sqlx` file. |
| `pre_operations { ... }` | `table`, `incremental`, `view` | SQL statements executed before the main table/view query runs (separated by `---` if multiple). |
| `post_operations { ... }` | `table`, `incremental`, `view` | SQL statements executed after the main table/view query completes (separated by `---` if multiple). |
| `incremental_where { ... }` | `incremental` | Optional SQL predicate expression used by `updatePartitionFilter` / incremental filtering. |
| `input "<target>" { ... }` | `test` | Mock SQL query replacing a referenced upstream dependency (`ref("<target>")`) during a unit test. |
| `---` | `operations`, `pre_operations`, `post_operations` | Statement separator for multi-statement SQL blocks. |

### SQLX Context Functions

Inside the SQL body and operation blocks of a `.sqlx` file (or via the `ctx` argument in JavaScript API callbacks), the following context methods are available:

| Function | Signature | Description |
|---|---|---|
| `ref` | `ref(name)` / `ref(schema, name)` / `ref(database, schema, name)` / `ref({ database?, schema?, name, includeDependentAssertions? })` | Resolves an action target to its backtick-quoted SQL identifier and registers it as an upstream dependency. |
| `resolve` | `resolve(name)` / `resolve(schema, name)` / `resolve(database, schema, name)` / `resolve(targetObj)` | Resolves an action target to its backtick-quoted SQL identifier **without** adding it as a dependency. |
| `self` | `self()` | Returns the fully qualified, backtick-quoted SQL identifier of the current action's target. |
| `name` | `name()` | Returns the finalized table/action name of the current action (including `namePrefix` if set). |
| `schema` | `schema()` | Returns the finalized dataset (`schema`) name of the current action (including `datasetSuffix` if set). |
| `database` | `database()` | Returns the finalized Google Cloud project (`database`) of the current action (including `projectSuffix` if set). |
| `when` | `when(condition, trueCase, falseCase = "")` | Returns `trueCase` when `condition` is truthy and `falseCase` otherwise (`table`, `incremental`, `view`, `assertion`). |
| `incremental` | `incremental()` | Returns `true` when compiling the incremental update query of an `incremental` table, or `false` when compiling its initial/full-refresh query (`table`, `incremental`, `view`). |

---

## Action Types Reference

Full field definitions for all action configurations are documented in the [Dataform Configs Reference](https://dataform-co.github.io/dataform/docs/reference/configs).

### `table`

Creates or replaces a BigQuery table (`CREATE OR REPLACE TABLE`). See [`TableConfig`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfig-TableConfig).

```sql
-- definitions/marts/orders_by_day.sqlx
config {
  type: "table",
  schema: "marts",
  description: "Daily aggregated order metrics",
  columns: {
    order_date: "UTC date of the order",
    total_orders: "Count of completed orders"
  },
  bigquery: {
    partitionBy: "order_date",
    clusterBy: ["country_code"],
    labels: {
      domain: "commerce"
    }
  },
  assertions: {
    uniqueKey: ["order_date", "country_code"],
    nonNull: ["order_date"]
  },
  tags: ["daily"]
}

SELECT
  DATE(order_timestamp) AS order_date,
  country_code,
  COUNT(*) AS total_orders
FROM ${ref("stg_orders")}
GROUP BY 1, 2
```

Key `table` config properties:
- **Target & metadata**: `name`, `schema` (or `dataset`), `database` (or `project`), `description`, `columns`, `tags`, `disabled`, `dependencies`, `dependOnDependencyAssertions`, `hermetic`, `metadata` (`overview`, `extraProperties`).
- **BigQuery options** (top-level or nested under `bigquery: {}`): `partitionBy`, `partitionExpirationDays`, `requirePartitionFilter`, `clusterBy`, `labels`, `additionalOptions`, `iceberg`, `preserveGovernanceControls`.
- **Execution & assertions**: `assertions` (`uniqueKey`, `uniqueKeys`, `nonNull`, `rowConditions`), `preOperations`, `postOperations`, `reservation`, `jobLabels`, `lakehouse`, `compilation_mode` (`"aot"` or `"jit"`).

---

### `incremental`

Builds a table from scratch on first run (or `--full-refresh`), and inserts or merges new rows on subsequent runs when the target table already exists. See [`IncrementalTableConfig`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfig-IncrementalTableConfig).

```sql
-- definitions/marts/events_incremental.sqlx
config {
  type: "incremental",
  uniqueKey: ["event_id"],
  bigquery: {
    partitionBy: "DATE(event_timestamp)",
    updatePartitionFilter: "event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"
  },
  protected: false
}

SELECT
  event_id,
  event_timestamp,
  user_id
FROM ${ref("raw_events")}
${when(incremental(), `WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM ${self()})`)}
```

Incremental-specific config properties (in addition to `table` properties):
- `uniqueKey`: Column name(s) used to `MERGE` rows on incremental updates; if omitted, new rows are appended (`INSERT`).
- `incrementalStrategy`: `"INCREMENTAL_STRATEGY_MERGE"` or `"INCREMENTAL_STRATEGY_INSERT_OVERWRITE"`.
- `updatePartitionFilter` / `incrementalPredicates`: SQL filter(s) restricting target partitions scanned during incremental updates.
- `onSchemaChange`: `"IGNORE"` (default), `"FAIL"`, `"EXTEND"`, or `"SYNCHRONIZE"`.
- `protected`: When `true`, prevents the table from being dropped and rebuilt from scratch on `--full-refresh`.

---

### `view`

Creates a virtual view (`CREATE OR REPLACE VIEW`) or a BigQuery materialized view (`materialized: true`). See [`ViewConfig`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfig-ViewConfig).

```sql
-- definitions/staging/stg_orders.sqlx
config {
  type: "view",
  materialized: true,
  bigquery: {
    partitionBy: "DATE(order_timestamp)",
    clusterBy: ["customer_id"]
  }
}

SELECT
  order_id,
  customer_id,
  order_timestamp
FROM ${ref("raw_orders")}
```

---

### `operations`

Executes arbitrary SQL statements (DDL, DML, stored procedures, `GRANT`/`REVOKE`). Multiple statements are separated by `---`. See [`OperationConfig`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfig-OperationConfig).

```sql
-- definitions/ops/cleanup_temp_tables.sqlx
config {
  type: "operations",
  hasOutput: false,
  dependencies: ["orders_by_day"]
}

DELETE FROM `my-gcp-project.staging.scratch_events`
WHERE created_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
```

When `hasOutput: true` is set, the operation registers a referenceable target at `database.schema.name` that downstream actions can reference with `${ref("<name>")}` or `${self()}`.

---

### `assertion` and Built-in Assertions

An assertion is a data quality query that **fails if it returns one or more rows**. Assertions materialize as views in `defaultAssertionDataset` (`dataform_assertions` by default). See [`AssertionConfig`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfig-AssertionConfig).

```sql
-- definitions/assertions/assert_positive_order_amount.sqlx
config {
  type: "assertion",
  tags: ["critical"]
}

SELECT order_id, amount
FROM ${ref("stg_orders")}
WHERE amount <= 0
```

You can also configure **built-in assertions** directly inside a `table`, `incremental`, or `view` `config` block:

```sql
config {
  type: "table",
  assertions: {
    uniqueKey: ["order_id"],
    nonNull: ["order_id", "customer_id"],
    rowConditions: [
      "amount >= 0",
      "order_timestamp <= CURRENT_TIMESTAMP()"
    ]
  }
}
```

---

### `declaration`

Declares an external BigQuery table or view (not managed by Dataform) so it can be referenced via `${ref("<name>")}`, documented, and shown in the dependency graph. See [`DeclarationConfig`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfig-DeclarationConfig).

```sql
-- definitions/sources/raw_orders.sqlx
config {
  type: "declaration",
  database: "my-raw-data-project",
  schema: "stripe",
  name: "raw_orders",
  description: "Raw Stripe orders landing table"
}
```

---

### `test` (Unit Tests)

Validates the SQL logic of a target `dataset` (`table`, `view`, or `incremental` action) by replacing its upstream `ref()` inputs with static mock queries (`input "<ref_name>" { ... }`) and comparing the output against an expected query. Executed via `dataform test` or `dataform run --run-tests`.

```sql
-- definitions/tests/orders_by_day_test.sqlx
config {
  type: "test",
  dataset: "orders_by_day"
}

input "stg_orders" {
  SELECT TIMESTAMP("2026-01-01 10:00:00 UTC") AS order_timestamp, "PL" AS country_code
  UNION ALL
  SELECT TIMESTAMP("2026-01-01 12:00:00 UTC") AS order_timestamp, "PL" AS country_code
}

SELECT DATE("2026-01-01") AS order_date, "PL" AS country_code, 2 AS total_orders
```

---

### `notebook`

Configures a Jupyter Notebook (`.ipynb`) action for execution in Google Cloud Dataform (via Vertex AI Colab Enterprise). See [`NotebookConfig`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfig-NotebookConfig).

```yaml
# definitions/actions.yaml
actions:
  - notebook:
      name: train_churn_model
      filename: notebooks/train_churn_model.ipynb
      dependencyTargets:
        - name: orders_by_day
```

---

### `propertyGraph` (`graph.yaml`)

Defines a BigQuery Property Graph (`CREATE OR REPLACE PROPERTY GRAPH`) from a `definitions/**/graph.yaml` file following the Google Semantic Ontology schema (`version: 0.2.0.dev0/google`). See the [BigQuery Property Graphs guide](https://github.com/dataform-co/dataform/blob/main/docs/property_graphs.md) for full syntax and examples.

---

### `actions.yaml` Action Configs

Plain `.sql` and `.ipynb` files can be registered as Dataform actions via `definitions/**/actions.yaml` files validated against [`dataform.ActionConfigs`](https://dataform-co.github.io/dataform/docs/reference/configs#dataform-ActionConfigs):

```yaml
# definitions/actions.yaml
actions:
  - declaration:
      name: raw_users
      project: my-raw-project
      dataset: raw
  - view:
      name: stg_users
      filename: staging/stg_users.sql
      dependencyTargets:
        - name: raw_users
  - table:
      name: dim_users
      filename: marts/dim_users.sql
      dependencyTargets:
        - name: stg_users
      partitionBy: created_date
```

---

## JavaScript API (`Session` Globals)

Inside `definitions/**/*.js` files (and `js {}` blocks in `.sqlx` files), Dataform attaches the active [`Session`](https://github.com/dataform-co/dataform/blob/main/docs/reference/session.md) instance to `global.dataform` and binds its action-builder methods as global functions:

| Global Function / Object | Signature | Description |
|---|---|---|
| `publish` | `publish(name, queryOrConfig?)` | Creates a `table`, `view` (default if `type` is omitted), or `incremental` action. Returns a `Table`, `View`, or `IncrementalTable` builder (`.query(ctx => ...)`). |
| `operate` | `operate(name, queriesOrConfig?)` | Creates an `operations` action. Returns an `Operation` builder (`.queries(ctx => ...)`). |
| `assert` | `assert(name, queryOrConfig?)` | Creates an `assertion` action. Returns an `Assertion` builder (`.query(ctx => ...)`). |
| `declare` | `declare(config)` | Declares an external data source (`{ database?, schema?, name, description?, columns? }`). |
| `test` | `test(name)` | Creates a unit test action (`.dataset(target).input(refName, sql).expect(sql)`). |
| `notebook` | `notebook(config)` | Creates a `Notebook` action (`{ name?, filename, location?, project?, dependencyTargets?, tags?, disabled?, description? }`). |
| `getContents` | `getContents(relativeMdPath)` | Reads a `.md` file inside the project directory (resolved relative to the caller file) and returns its contents as a string for `description` fields. |
| `jitData` | `jitData(key, value)` | Registers structured data in `CompiledGraph.jitData` for Just-in-Time (JiT) compilation workflows. |
| `dataform.projectConfig` | `dataform.ProjectConfig` | Read-only access to the resolved project configuration, including custom variables (`dataform.projectConfig.vars`). |

Example (`definitions/generate_staging_views.js`):

```js
const sourceTables = ["orders", "customers", "products"];

sourceTables.forEach((tableName) => {
  declare({
    schema: "raw",
    name: `raw_${tableName}`,
  });

  publish(`stg_${tableName}`, {
    type: "view",
    schema: "staging",
    description: getContents(`./docs/stg_${tableName}.md`),
  }).query((ctx) => `SELECT * FROM ${ctx.ref("raw", `raw_${tableName}`)}`);
});
```

---

## Package Exports and Bundled `.proto` Files

### TypeScript / JavaScript Exports

When imported as an npm library (`import * as dataformCore from "@dataform/core"`), `@dataform/core` exports the following symbols (with TypeScript typings in `bundle.d.ts`):

- **`version`** (`string`): The installed `@dataform/core` semantic version string (for example, `"3.1.5"`).
- **`session`** (`Session`): The singleton compilation `Session` instance.
- **`compiler`** (`(code: string, path: string) => string`): Transpiles `.sqlx`, `.yaml`/`.yml`, `.sql`, `.ipynb`, and `.md` source strings into executable JavaScript modules.
- **`main`** (`(coreExecutionRequest: Uint8Array | string) => Uint8Array | string`): Entry point invoked by the compilation VM sandbox; accepts an encoded `dataform.CoreExecutionRequest` proto and returns an encoded `dataform.CoreExecutionResponse` proto containing `CompiledGraph`.
- **`jitCompiler`** (`IJitCompiler`): Just-in-Time (JiT) action compiler entry point.
- **`supportedFeatures`** (`dataform.SupportedFeatures[]`): Feature capability flags supported by this `@dataform/core` version (such as `ARRAY_BUFFER_IPC`).
- **`IDataformExtension`** / **`IJitCompiler`**: TypeScript interfaces for extensions and JiT compilation.

### Bundled Protocol Buffer Schemas

The published `@dataform/core` package includes the canonical `.proto` definitions at the package root:

- `core.proto` — `CompiledGraph`, `Table`, `Operation`, `Assertion`, `Declaration`, `Test`, `Notebook`, `DataPreparation`, `PropertyGraph`, `Target`, `ProjectConfig`, `CoreExecutionRequest`, `CoreExecutionResponse`.
- `configs.proto` — `WorkflowSettings`, `ActionConfigs`, `ActionConfig` (`TableConfig`, `ViewConfig`, `IncrementalTableConfig`, `AssertionConfig`, `OperationConfig`, `DeclarationConfig`, `NotebookConfig`, `DataPreparationConfig`).
- `property_graph_config.proto` — `PropertyGraphConfig` (`SemanticModel`, `Entity`, `Relationship`, `Deployment`).
- `db_adapter.proto` & `jit.proto` — Database adapter and Just-in-Time (JiT) compilation RPC messages.
- `extension.proto` — `Extension` configuration schema.

---

## Documentation and Links

- [Dataform Documentation Home](https://cloud.google.com/dataform/docs/overview)
- [Dataform Core Reference (Google Cloud Docs)](https://cloud.google.com/dataform/docs/reference/dataform-core-reference)
- [Dataform Configs Reference (`configs.proto`)](https://dataform-co.github.io/dataform/docs/reference/configs)
- [Dataform JavaScript `Session` Reference](https://github.com/dataform-co/dataform/blob/main/docs/reference/session.md)
- [Dataform CLI Reference (`@dataform/cli`)](https://github.com/dataform-co/dataform/blob/main/cli/README.md)
- [BigQuery Property Graphs Guide (`graph.yaml`)](https://github.com/dataform-co/dataform/blob/main/docs/property_graphs.md)
- [Knowledge Catalog Data Lineage Guide](https://github.com/dataform-co/dataform/blob/main/docs/lineage.md)
- [Stateless Installs Guide (`.dataformignore`)](https://github.com/dataform-co/dataform/blob/main/docs/stateless-install.md)
- [GitHub Repository & Issue Tracker](https://github.com/dataform-co/dataform)
