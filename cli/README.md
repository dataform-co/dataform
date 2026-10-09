# Dataform CLI Reference

This document is a complete reference for the `dataform` command line interface, generated from the CLI source in this repository (`cli/`). It covers every command and flag as implemented today, plus practical example use cases.

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

- [`init`](#init)
- [`install`](#install)
- [`init-creds`](#init-creds)
- [`compile`](#compile)
- [`test`](#test)
- [`run`](#run)
- [`format`](#format)

---

### `init`

```
dataform init [project-dir] [default-database] [default-location]
```

Creates a new Dataform project (`workflow_settings.yaml`, `.gitignore`, `definitions/`, and `includes/`) in `project-dir`. Fails if `workflow_settings.yaml`, `package.json`, or `dataform.json` already exists in `project-dir`.

**Positional arguments**

| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The directory to create the project in. |
| `default-database` | **Yes** | The default database to use, equivalent to a Google Cloud Project ID. |
| `default-location` | **Yes** | The default BigQuery location. See [supported values](https://cloud.google.com/bigquery/docs/locations). |

`default-database` and `default-location` are positional but enforced as required at runtime — omitting either produces an error telling you to run `dataform help init`. All three positional arguments can also be passed as named flags (`--project-dir`, `--default-database`, `--default-location`), e.g. `dataform init my_project --default-database=my-gcp-project-id --default-location=US`.

**Options**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--iceberg` | boolean | `false` | Initialize the project with workflow-level Iceberg tables configuration. When set, prompts interactively for an Iceberg bucket name, table folder root/subpath, and connection. |

---

### `install`

```
dataform install [project-dir]
```

Installs a project's NPM dependencies (via `npm i --ignore-scripts`) as declared in the project's `package.json`.

**Positional arguments**

| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |

**Options:** none.

> Note: for projects that specify `dataformCoreVersion` in `workflow_settings.yaml`, `@dataform/core` is installed automatically in an isolated temporary directory at compile/run time, and running `dataform install` throws an error (`No installation is needed when using workflow_settings.yaml, as packages are installed at runtime.`). Use `dataform install` only when managing `@dataform/core` and external dependencies via `package.json` (without `dataformCoreVersion` in `workflow_settings.yaml`).

---

### `init-creds`

```
dataform init-creds [project-dir]
```

Interactively creates a `.df-credentials.json` file that Dataform uses to connect to BigQuery. **Only BigQuery is currently supported.**

Prompts for:
1. Dataset location (`US`, `EU`, or a custom region name).
2. Authentication method — Application Default Credentials (ADC) or a JSON service account key file.
   - ADC: prompts for your billing project ID.
   - JSON key: prompts for the path to a downloaded service account key file.

**Positional arguments**

| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory the credentials file is written into. |

**Options**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--test-connection` | boolean | `true` | If true, runs a test query using the credentials you just entered before saving them. |

---

### `compile`

```
dataform compile [project-dir]
```

Compiles the project and prints a summary (or JSON / Graphviz DOT representation) of the compiled dependency graph without executing it against your warehouse. Useful for validating a project and inspecting its structure.

**Positional arguments**

| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |

**Options**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--json` | boolean | `false` | Outputs a JSON representation of the compiled project. Mutually exclusive with `--dot`. |
| `--dot` | boolean | `false` | Outputs a [Graphviz DOT](https://graphviz.org/doc/info/lang.html) representation of the compiled project. Mutually exclusive with `--json`. |
| `--verbose` | boolean | `false` | Enable verbose compilation output. Mutually exclusive with `--quiet`. |
| `--quiet` | boolean | `false` | Less verbose compilation output. Mutually exclusive with `--verbose`. |
| `--watch` | boolean | `false` | Watches the project directory (ignoring `node_modules`) and recompiles automatically on changes, with a debounce of 500ms. Runs until interrupted with Ctrl+C. |
| `--timeout` | string | `'60s'` (`60000` ms) | Duration to allow compilation to complete, e.g. `'1s'`, `'10m'`. |
| `--output-actions` | array | — | Filters the *printed* output to only these actions (comma-separated or repeatable). The whole project still compiles; this only prunes what's printed. Each entry must be either an unambiguous bare action `<name>` or a fully-qualified `<database>.<schema>.<name>` target (no `*` wildcard expansion). |
| `--output-tags` | array | — | Filters the printed output to actions with any of these tags (comma-separated or repeatable). |
| `--output-include-deps` | boolean | — | Also includes upstream dependencies of the selected actions in the output. Requires `--output-actions` or `--output-tags`. |
| `--output-include-dependents` | boolean | — | Also includes downstream dependents of the selected actions in the output. Requires `--output-actions` or `--output-tags`. |
| Project-config overrides | — | — | See [Shared project-config flags](#shared-project-config-flags). |

---

### `test`

```
dataform test [project-dir]
```

Compiles the project and runs its unit tests (`type: "test"` actions) against BigQuery.

**Positional arguments**

| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |

**Options**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--credentials` | string | `.df-credentials.json` | Path to the credentials JSON file to use. |
| `--timeout` | string | `'60s'` (`60000` ms) | Duration to allow compilation to complete, e.g. `'1s'`, `'10m'`. |
| `--json` | boolean | `false` | Outputs test results as JSON instead of human-readable text. |
| Project-config overrides | — | — | See [Shared project-config flags](#shared-project-config-flags). |

Exits with code `1` if any test fails, if the project fails to compile, or if no unit tests are found. Exits `0` if all tests pass.

---

### `run`

```
dataform run [project-dir]
```

Compiles and executes the project against BigQuery (or validates it with `--dry-run`).

**Positional arguments**

| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |

**Options**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--dry-run` | boolean | — | Validates the run SQL with BigQuery without applying any changes to the warehouse. |
| `--run-tests` | boolean | — | Requires the project's unit tests to pass before executing; aborts the run if any test fails. |
| `--action-retry-limit` | number | `0` | Retries idempotent actions up to this many times on failure. |
| `--actions` | array | — | Action names to run (comma-separated or repeatable). Each entry must be either an unambiguous bare action `<name>` or a fully-qualified `<database>.<schema>.<name>` target (note: despite the CLI help text, `*` wildcards are not expanded). |
| `--tags` | array | — | Tags to filter the actions to run (comma-separated or repeatable). |
| `--include-deps` | boolean | — | Also runs upstream dependencies of the selected actions. Requires `--actions` or `--tags`. |
| `--include-dependents` | boolean | — | Also runs downstream dependents of the selected actions. Requires `--actions` or `--tags`. |
| `--full-refresh` | boolean | `false` | Forces incremental tables (unless marked `protected`) to be rebuilt from scratch instead of updated incrementally (regular tables and views are always rebuilt). |
| `--credentials` | string | `.df-credentials.json` | Path to the credentials JSON file to use. |
| `--json` | boolean | `false` | Outputs the run result as JSON. Only supported together with `--dry-run` — plain `run --json` (without `--dry-run`) is rejected. |
| `--timeout` | string | `'60s'` (`60000` ms) | Duration to allow **compilation only** to complete, e.g. `'1s'`, `'10m'`. Does not bound execution — see `--execution-timeout`. |
| `--execution-timeout` | string | — | Wall-clock deadline for the *entire* run (compile + all actions), e.g. `'10m'`, `'2h'`. When it fires, in-flight actions are cancelled and pending actions are skipped. Off by default. |
| `--jit-timeout` | string | — | Per-model just-in-time (JiT) compilation worker timeout, e.g. `'30s'`, `'2m'`. Each action with JiT code gets its own fresh deadline, independent of `--execution-timeout`. If unset, only `--execution-timeout` bounds JiT work. |
| `--job-prefix` | string | — | Adds an additional prefix to BigQuery job IDs, in the form `dataform-${jobPrefix}-`. |
| `--job-labels` | string | — | Comma-separated `key=value` labels to attach to BigQuery jobs, e.g. `'key1=val1,key2=val2'`. |
| `--emit-lineage` | boolean | — | Emits OpenLineage `RunEvent`s to Knowledge Catalog Lineage for each executed action. Overrides the `lineage.enabled` setting in `workflow_settings.yaml` when specified. |
| Project-config overrides | — | — | See [Shared project-config flags](#shared-project-config-flags). |

If `--timeout` is set without `--execution-timeout`, `run` prints a warning reminding you that `--timeout` only bounds compilation.

Exit codes: `0` on success, `1` if compilation fails, unit tests fail (with `--run-tests`), the run fails, times out, or is cancelled (Ctrl+C).

---

### `format`

```
dataform format [project-dir]
```

Formats `.sqlx` and `.js` files in the project using Dataform's built-in formatter.

**Positional arguments**

| Argument | Required | Description |
|---|---|---|
| `project-dir` | No (default `.`) | The Dataform project directory. |

**Options**

| Flag | Type | Default | Description |
|---|---|---|---|
| `--actions` | array | `{definitions,includes}/**/*.{js,sqlx}` | Glob pattern(s) of files to format, relative to `project-dir`. (Note: despite the shared flag name, here it means file globs, not action names.) |
| `--ignore-js-files` | boolean | `false` | If set, only formats `.sqlx` files (skips `.js` files). |
| `--check` | boolean | `false` | Checks whether files are already formatted correctly, without modifying them. Exits `1` if any file would be reformatted. |

---

## Shared project-config flags

`compile`, `test`, and `run` all accept the following flags, which override the equivalent settings in the project's `workflow_settings.yaml` for that invocation only:

| Flag | Type | Description |
|---|---|---|
| `--default-database` | string | Overrides the default database (Google Cloud Project ID). |
| `--default-schema` | string | Overrides the default schema name. |
| `--default-location` | string | Overrides the default BigQuery location. See [supported values](https://cloud.google.com/bigquery/docs/locations). |
| `--assertion-schema` | string | Overrides the default assertion schema. |
| `--vars` | string | Injects variables as `key=value` pairs (comma-separated), referenced in code via `dataform.projectConfig.vars.someKey`. Example: `--vars=env=staging,region=eu`. |
| `--database-suffix` | string | Suffix appended to all output database names. |
| `--schema-suffix` | string | Suffix appended to all output schema names. Must contain only alphanumeric characters and/or underscores. |
| `--table-prefix` | string | Prefix added to all table names. |
| `--disable-assertions` | boolean | Disables all assertions, both built-in (`uniqueKey`, `nonNull`, `rowConditions`) and manual (`type: assertion`). Default `false`. |
| `--default-reservation` | string | The default BigQuery reservation to use for execution. |

## Other shared flags

These are reused by multiple commands:

| Flag | Used by | Description |
|---|---|---|
| `--actions` | `run`, `compile` (as `--output-actions`/`--output-tags` under different names), `format` | Meaning varies by command — see each command's table above. |
| `--credentials` | `test`, `run` | Path to the credentials JSON file. Default: `.df-credentials.json`. |
| `--json` | `compile`, `test`, `run` | Requests JSON output instead of human-readable text. |
| `--timeout` | `compile`, `test`, `run` | Bounds project **compilation** time only (default `60s`). |

## Credentials file

`init-creds` writes a `.df-credentials.json` file (filename fixed, not configurable) into the project directory. This file stores the connection details Dataform needs to reach your warehouse:

- **ADC-based**: `{ "projectId": ..., "location": ... }`
- **JSON-key-based**: `{ "projectId": ..., "credentials": "<service account key JSON as a string>", "location": ... }`

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
