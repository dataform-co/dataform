# Dataform CLI Reference

<!--
  The command list, usage lines and argument/option tables in this file are generated from the CLI
  command definitions in cli/. Don't edit the generated blocks by hand: change the definitions
  (e.g. an option's `describe`) and run `bazel run //cli:update_readme`.
-->

This document is a complete reference for the `dataform` command line interface. The command list, usage lines and argument/option tables are generated from the CLI source in this repository (`cli/`), so they match `dataform help`. It also covers practical example use cases.

For conceptual documentation (project structure, SQLX syntax, the JavaScript API, etc.) see the [root readme](../readme.md) and the hosted [Dataform CLI guide](https://cloud.google.com/dataform/docs/use-dataform-cli).

## Installation

```bash
npm i -g @dataform/cli
```

## Invocation

```
dataform <command> [project-dir] [options]
```

`project-dir` is optional for every command and defaults to `.` (the current directory). It is resolved with tilde-expansion, so `~/my-project` works. `project-dir` can be passed either positionally (`dataform compile my-project`) or as a named flag (`dataform compile --project-dir=my-project`).

Run `dataform` with no arguments, or `dataform help`, to print the list of commands. Run `dataform help <command>` or `dataform <command> --help` for a command's full flag list.

## Global behavior

- **`--help`** — shows help for the current command.
- **`--version`** — prints the installed `@dataform/cli` version number.
- **Strict parsing** — unknown commands or flags are rejected with an error, and the CLI suggests the closest matching command name for typos.
- **Colored output** — output is colorized by default. Set `NO_COLOR=1` (or `true`/`yes`) to disable it.
- **Lineage debug logging** — set `DATAFORM_LINEAGE_DEBUG=1` to enable verbose logging from the OpenLineage emitter used by `run --emit-lineage`.

## Commands

<!-- BEGIN GENERATED: commands -->
- [`init`](#init)
- [`install`](#install)
- [`init-creds`](#init-creds)
- [`compile`](#compile)
- [`test`](#test)
- [`run`](#run)
- [`format`](#format)
<!-- END GENERATED: commands -->

---

### `init`

<!-- BEGIN GENERATED: usage init -->
```
dataform init [project-dir] [default-database] [default-location]
```
<!-- END GENERATED: usage init -->

Creates a new Dataform project (`workflow_settings.yaml`, `.gitignore`, `definitions/`, and `includes/`) in `project-dir`. Fails if `workflow_settings.yaml`, `package.json`, or `dataform.json` already exists in `project-dir`.

**Positional arguments**

<!-- BEGIN GENERATED: positionals init -->
| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |
| `default-database` | **Yes** | The default database to use, equivalent to Google Cloud Project ID. |
| `default-location` | No | The default location to use (optional). If omitted, \`defaultLocation\` is not written to \`workflow_settings.yaml\` and the \`location\` from \`.df-credentials.json\` is used instead; if that is also unset, BigQuery chooses the location dynamically. See https://cloud.google.com/bigquery/docs/locations for supported values. |
<!-- END GENERATED: positionals init -->

`project-dir` is the directory to create the project in. All three positional arguments can also be passed as named flags (`--project-dir`, `--default-database`, `--default-location`), e.g. `dataform init my_project --default-database=my-gcp-project-id --default-location=US`.

**Options**

<!-- BEGIN GENERATED: options init -->
| Flag | Type | Default | Description |
|---|---|---|---|
| `--iceberg` | boolean | `false` | Initialize the project with workflow-level Iceberg tables configuration. |
<!-- END GENERATED: options init -->

When `--iceberg` is set, `init` prompts interactively for an Iceberg bucket name, table folder root/subpath, and connection.

---

### `install`

<!-- BEGIN GENERATED: usage install -->
```
dataform install [project-dir]
```
<!-- END GENERATED: usage install -->

Installs a project's NPM dependencies (via `npm i --ignore-scripts`) as declared in the project's `package.json`.

**Positional arguments**

<!-- BEGIN GENERATED: positionals install -->
| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |
<!-- END GENERATED: positionals install -->

**Options**

<!-- BEGIN GENERATED: options install -->
None.
<!-- END GENERATED: options install -->

> Note: for projects that specify `dataformCoreVersion` in `workflow_settings.yaml`, `@dataform/core` is installed automatically in an isolated temporary directory at compile/run time, and running `dataform install` throws an error (`No installation is needed when using workflow_settings.yaml, as packages are installed at runtime.`). Use `dataform install` only when managing `@dataform/core` and external dependencies via `package.json` (without `dataformCoreVersion` in `workflow_settings.yaml`).

---

### `init-creds`

<!-- BEGIN GENERATED: usage init-creds -->
```
dataform init-creds [project-dir]
```
<!-- END GENERATED: usage init-creds -->

Interactively creates a `.df-credentials.json` file in `project-dir` that Dataform uses to connect to BigQuery. **Only BigQuery is currently supported.**

Prompts for:
1. Dataset location (`US`, `EU`, a custom region name, or `auto`). Choosing `auto` leaves `location` out of the credentials file so BigQuery chooses the location dynamically.
2. Authentication method — Application Default Credentials (ADC) or a JSON service account key file.
   - ADC: prompts for your billing project ID.
   - JSON key: prompts for the path to a downloaded service account key file.
3. A service account to impersonate (optional, press Enter to skip). It's saved as `impersonateServiceAccount`, see [Credentials file](#credentials-file).

`init-creds` does not prompt for a universe domain. When the JSON key contains `universe_domain`, that value is saved as `universeDomain`. To target a non-default universe with ADC, set `universeDomain` by hand in `.df-credentials.json`.

**Positional arguments**

<!-- BEGIN GENERATED: positionals init-creds -->
| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |
<!-- END GENERATED: positionals init-creds -->

**Options**

<!-- BEGIN GENERATED: options init-creds -->
| Flag | Type | Default | Description |
|---|---|---|---|
| `--test-connection` | boolean | `true` | If true, a test query will be run using your final credentials. |
<!-- END GENERATED: options init-creds -->

---

### `compile`

<!-- BEGIN GENERATED: usage compile -->
```
dataform compile [project-dir]
```
<!-- END GENERATED: usage compile -->

Compiles the project and prints a summary (or JSON / Graphviz DOT representation) of the compiled dependency graph without executing it against your warehouse. Useful for validating a project and inspecting its structure.

**Positional arguments**

<!-- BEGIN GENERATED: positionals compile -->
| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |
<!-- END GENERATED: positionals compile -->

**Options**

<!-- BEGIN GENERATED: options compile -->
| Flag | Type | Default | Description |
|---|---|---|---|
| `--json` | boolean | `false` | Outputs the result as JSON instead of human-readable text. |
| `--dot` | boolean | `false` | Outputs a Graphviz DOT representation of the compiled project. Mutually exclusive with --json. |
| `--verbose` | boolean | `false` | Enables verbose compilation output. Mutually exclusive with --quiet. |
| `--quiet` | boolean | `false` | Less verbose compilation output. Mutually exclusive with --verbose. |
| `--watch` | boolean | `false` | Watches the project directory and recompiles on changes. |
| `--timeout` | string | `'60s'` | Duration to allow project compilation to complete, e.g. '1s', '10m'. |
| `--output-actions` | array | — | Filters the printed output to these actions (comma-separated or repeatable). Each entry is an action name or a fully-qualified database.schema.name target. The whole project is still compiled. |
| `--output-tags` | array | — | Filters the printed output to actions with any of these tags (comma-separated or repeatable). |
| `--output-include-deps` | boolean | — | Also includes upstream dependencies of the selected actions in the output. Requires --output-actions or --output-tags. |
| `--output-include-dependents` | boolean | — | Also includes downstream dependents of the selected actions in the output. Requires --output-actions or --output-tags. |
| Project-config overrides | — | — | See [Shared project-config flags](#shared-project-config-flags). |
<!-- END GENERATED: options compile -->

**Notes**

- `--json` outputs the compiled graph and can't be combined with `--dot`.
- `--watch` ignores `node_modules`, recompiles with a debounce of 500ms, and runs until interrupted with Ctrl+C.
- `--output-actions`, `--output-tags`, `--output-include-deps` and `--output-include-dependents` only prune what's printed; the whole project still compiles. `--output-actions` entries are matched exactly (no `*` wildcard expansion).

---

### `test`

<!-- BEGIN GENERATED: usage test -->
```
dataform test [project-dir]
```
<!-- END GENERATED: usage test -->

Compiles the project and runs its unit tests (`type: "test"` actions) against BigQuery.

**Positional arguments**

<!-- BEGIN GENERATED: positionals test -->
| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |
<!-- END GENERATED: positionals test -->

**Options**

<!-- BEGIN GENERATED: options test -->
| Flag | Type | Default | Description |
|---|---|---|---|
| `--credentials` | string | `.df-credentials.json` | Path to the credentials JSON file to use. |
| `--timeout` | string | `'60s'` | Duration to allow project compilation to complete, e.g. '1s', '10m'. |
| `--json` | boolean | `false` | Outputs the result as JSON instead of human-readable text. |
| Project-config overrides | — | — | See [Shared project-config flags](#shared-project-config-flags). |
<!-- END GENERATED: options test -->

`--json` outputs the test results as JSON.

Exits with code `1` if any test fails, if the project fails to compile, or if no unit tests are found. Exits `0` if all tests pass.

---

### `run`

<!-- BEGIN GENERATED: usage run -->
```
dataform run [project-dir]
```
<!-- END GENERATED: usage run -->

Compiles and executes the project against BigQuery (or validates it with `--dry-run`).

**Positional arguments**

<!-- BEGIN GENERATED: positionals run -->
| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |
<!-- END GENERATED: positionals run -->

**Options**

<!-- BEGIN GENERATED: options run -->
| Flag | Type | Default | Description |
|---|---|---|---|
| `--dry-run` | boolean | — | Validates the run SQL with BigQuery without applying any changes to the warehouse. |
| `--run-tests` | boolean | — | Requires the project's unit tests to pass before executing; aborts the run if any test fails. |
| `--action-retry-limit` | number | `0` | Retries idempotent actions up to this many times on failure. |
| `--actions` | array | — | Actions to run (comma-separated or repeatable). Each entry is an action name or a fully-qualified database.schema.name target. |
| `--tags` | array | — | Tags to filter the actions to run (comma-separated or repeatable). |
| `--include-deps` | boolean | — | Also runs upstream dependencies of the selected actions. Requires --actions or --tags. |
| `--include-dependents` | boolean | — | Also runs downstream dependents of the selected actions. Requires --actions or --tags. |
| `--full-refresh` | boolean | `false` | Forces incremental tables (unless marked protected) to be rebuilt from scratch. |
| `--credentials` | string | `.df-credentials.json` | Path to the credentials JSON file to use. |
| `--json` | boolean | `false` | Outputs the result as JSON instead of human-readable text. |
| `--timeout` | string | `'60s'` | Duration to allow project compilation to complete, e.g. '1s', '10m'. |
| `--execution-timeout` | string | — | Wall-clock deadline for the entire run (compile + all actions). When it fires, in-flight actions are cancelled and pending actions are skipped. Off by default. Examples: '10m', '2h'. |
| `--jit-timeout` | string | — | Per-model JiT compilation worker timeout. Each JiT action gets its own fresh deadline, independent of --execution-timeout. When unset, no per-model cap is applied and only --execution-timeout bounds JiT work. Examples: '30s', '2m'. |
| `--job-prefix` | string | — | Adds an additional prefix to BigQuery job IDs, in the form dataform-&lt;job-prefix&gt;-. |
| `--job-labels` | string | — | Comma-separated list of labels to add to BigQuery jobs, e.g. 'key1=val1,key2=val2'. Also applied to unit tests with --run-tests. Overrides job labels and table labels that use the same key. |
| `--emit-lineage` | boolean | — | If set, emits OpenLineage RunEvents to Knowledge Catalog Lineage for each executed action. Overrides workflow_settings.yaml lineage.enabled when specified. See https://github.com/dataform-co/dataform/blob/main/docs/lineage.md. |
| `--build-only` | boolean | `false` | Only builds the execution graph without submitting any queries to BigQuery. JiT actions are not compiled. Note that --dry-run and --run-tests will be ignored if combined with --build-only. |
| Project-config overrides | — | — | See [Shared project-config flags](#shared-project-config-flags). |
<!-- END GENERATED: options run -->

**Notes**

- `--actions` entries are matched exactly: `*` wildcards are not expanded, and a bare action name that matches several actions is an error.
- `--full-refresh` only affects incremental tables; regular tables and views are always rebuilt.
- `--json` outputs the run result as JSON and is only supported together with `--dry-run`; plain `run --json` is rejected.
- `--timeout` bounds compilation only, not execution. If it's set without `--execution-timeout`, `run` prints a warning reminding you of that.
- `--job-labels` override `defaultJobLabels`, action `jobLabels` and table `labels` that use the same key.
- `--emit-lineage` requires the Data Lineage API (`datalineage.googleapis.com`) to be enabled and the `roles/datalineage.producer` role; for configuration and troubleshooting, see the [Data Lineage guide](../docs/lineage.md).

Exit codes: `0` on success, `1` if compilation fails, a BigQuery job would get more than 64 labels (checked before any action or unit test runs), unit tests fail (with `--run-tests`), the run fails, times out, or is cancelled (Ctrl+C).

---

### `format`

<!-- BEGIN GENERATED: usage format -->
```
dataform format [project-dir]
```
<!-- END GENERATED: usage format -->

Formats `.sqlx` and `.js` files in the project using Dataform's built-in formatter.

**Positional arguments**

<!-- BEGIN GENERATED: positionals format -->
| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |
<!-- END GENERATED: positionals format -->

**Options**

<!-- BEGIN GENERATED: options format -->
| Flag | Type | Default | Description |
|---|---|---|---|
| `--actions` | array | `{definitions,includes}/**/*.{js,sqlx}` | Glob patterns of files to format, relative to the project directory (comma-separated or repeatable). |
| `--ignore-js-files` | boolean | `false` | If set, only formats .sqlx files (skips .js files). |
| `--check` | boolean | `false` | Checks whether files are already formatted correctly, without modifying them. |
<!-- END GENERATED: options format -->

**Notes**

- Despite the shared flag name, `--actions` here takes file globs relative to `project-dir`, not action names. With `--ignore-js-files`, the default becomes `{definitions,includes}/**/*.sqlx`.
- `--check` exits with code `1` if any file would be reformatted.

---

## Shared project-config flags

`compile`, `test`, and `run` all accept the following flags, which override the equivalent settings in the project's `workflow_settings.yaml` for that invocation only:

<!-- BEGIN GENERATED: project-config-options -->
| Flag | Type | Default | Description |
|---|---|---|---|
| `--default-database` | string | — | The default database to use, equivalent to Google Cloud Project ID. If unset, the value from workflow_settings.yaml is used. |
| `--default-schema` | string | — | Override for the default schema name. If unset, the value from workflow_settings.yaml is used. |
| `--default-location` | string | — | The default location to use. See https://cloud.google.com/bigquery/docs/locations for supported values. If unset, the value from workflow_settings.yaml is used. |
| `--assertion-schema` | string | — | Default assertion schema. If unset, the value from workflow_settings.yaml is used. |
| `--vars` | string | — | Variables to inject, as comma-separated key=value pairs (e.g. --vars=env=staging,region=eu), referenced in code via dataform.projectConfig.vars.&lt;key&gt;. If unset, the value from workflow_settings.yaml is used. |
| `--database-suffix` | string | — | A suffix to be appended to output database names. If unset, the value from workflow_settings.yaml is used. |
| `--schema-suffix` | string | — | A suffix to be appended to output schema names. Must contain only alphanumeric characters and/or underscores. If unset, the value from workflow_settings.yaml is used. |
| `--table-prefix` | string | — | Adds a prefix for all table names. If unset, the value from workflow_settings.yaml is used. |
| `--disable-assertions` | boolean | `false` | Disables all assertions including built-in assertions (uniqueKey, nonNull, rowConditions) and manual assertions (type: assertion). |
| `--default-reservation` | string | — | The default BigQuery reservation to use for execution. If unset, the value from workflow_settings.yaml is used. If neither is set, default BigQuery behavior applies. |
<!-- END GENERATED: project-config-options -->

## Credentials file

`init-creds` writes a `.df-credentials.json` file (filename fixed, not configurable) into the project directory. This file stores the connection details Dataform needs to reach your warehouse:

- **ADC-based**: `{ "projectId": ..., "location": ... }`
- **JSON-key-based**: `{ "projectId": ..., "credentials": "<service account key JSON as a string>", "location": ... }`

Additional details:

- **Location** is optional in both of the above (`init-creds` omits it if `auto` is chosen)
- **Service account impersonation** (optional, with either of the above): add `"impersonateServiceAccount": "<service account email>"` to run BigQuery queries (and lineage emission) as that service account. Your ADC identity, or the key above, is used to impersonate it and needs `roles/iam.serviceAccountTokenCreator` on that service account. `init-creds` asks for it as an optional last step.
- **Universe domain** (optional): add `"universeDomain": "s3nsapis.fr"` when BigQuery is in a non-default universe, such as a Trusted Partner Cloud. Leave it unset for the Google Default Universe. It can be combined with `impersonateServiceAccount`. Lineage emission is skipped when this field is set, because Cloud Data Lineage is not available outside the Google Default Universe.

Only BigQuery is supported by `init-creds` and the bundled BigQuery adapter in this codebase. Treat `.df-credentials.json` as a secret — `dataform init` adds it to `.gitignore` automatically; never commit it.

---

## Example use cases

### First-time project setup

```bash
dataform init my_project my-gcp-project-id US
cd my_project
dataform init-creds
dataform run
```

### Compiling and inspecting a project without touching the warehouse

```bash
# Human-readable summary
dataform compile

# Full compiled graph as JSON, e.g. to pipe into jq
dataform compile --json | jq '.tables[].target'

# Visualize the dependency graph
dataform compile --dot > graph.dot && dot -Tpng graph.dot -o graph.png

# Only show a subset of the graph (by unambiguous action name or full database.schema.name target)
dataform compile --output-actions=my_table --output-include-deps
dataform compile --output-actions=my-gcp-project-id.my_schema.my_table --output-include-deps
```

### Running unit tests before deploying changes

```bash
dataform test
# ...or fail a run early if tests don't pass:
dataform run --run-tests
```

### Running only part of a pipeline

```bash
# Run a specific table (by bare name or full database.schema.name) and anything downstream of it
dataform run --actions=my_table --include-dependents
dataform run --actions=my-gcp-project-id.my_schema.my_table --include-dependents

# Run everything tagged "daily", including their dependencies
dataform run --tags=daily --include-deps

# Force a full rebuild of the "my_incremental_table" incremental table
dataform run --actions=my_incremental_table --full-refresh
```

### Validating a run without applying changes

```bash
dataform run --dry-run --json > dry-run-result.json
```

### Bounding how long a run is allowed to take

```bash
# Compilation must finish within 30s; the whole run (compile + execute) within 15 minutes
dataform run --timeout=30s --execution-timeout=15m

# Cap each JiT-compiled model's own compilation step at 1 minute
dataform run --execution-timeout=15m --jit-timeout=1m
```

### Targeting a different environment for one invocation

```bash
dataform run \
  --default-database=my-staging-project \
  --schema-suffix=staging \
  --vars=env=staging,region=eu
```

### Iterating on SQLX during development

```bash
dataform compile --watch
```

### Checking formatting without modifying files

```bash
dataform format --check
```

### Tracking and labeling jobs from a run

```bash
dataform run --job-prefix=nightly --job-labels=team=analytics,pipeline=nightly-etl
```

### Emitting lineage events for a run

```bash
DATAFORM_LINEAGE_DEBUG=1 dataform run --emit-lineage
```

Lineage events are emitted for every action that runs a BigQuery job: tables (including views and incremental tables), operations, assertions and property graphs. On `COMPLETE` events, `inputs` and `outputs` are populated for actions that materialize a BigQuery object (`table`, `view`, `incremental`, `assertion`, `propertyGraph`, and `operations` with `hasOutput: true`). Assertions appear as a process for the assertion view written to the assertions dataset, with the assertion's dependencies reported as inputs. Property graph inputs are populated from the action's dependencies (`dependencyTargets`), so node and edge tables referenced via `ref` are reported as inputs while raw table paths in the graph definition are not. Declarations are not emitted. The action type is reported in the `gcp_bq_pipelines_job.actionType` attribute as `TABLE`, `OPERATIONS`, `ASSERTION` or `PROPERTY_GRAPH`, matching the values used by Dataform in Google Cloud. For prerequisites, `workflow_settings.yaml` configuration, and troubleshooting, see the [Data Lineage guide](../docs/lineage.md).

Lineage events are sent to the location set by `defaultLocation` in `workflow_settings.yaml` or, if that is not set, by `location` in `.df-credentials.json`. If neither is set, lineage emission is skipped with a warning and the run proceeds normally.
