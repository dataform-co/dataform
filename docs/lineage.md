# Data Lineage in Dataform CLI

[<- Back to Home](https://dataform-co.github.io/dataform) · [CLI Reference](../cli/README.md) · [Configs Reference](reference/configs.md#dataform-LineageSettings)

When lineage emission is enabled, `dataform run` emits [OpenLineage](https://openlineage.io/) `RunEvent` messages (`START` and terminal `COMPLETE`, `FAIL`, or `ABORT`) to the Google Cloud [Data Lineage API](https://cloud.google.com/data-catalog/docs/concepts/about-data-lineage) (`datalineage.googleapis.com`) for each executed action. Events are ingested under the `BIGQUERY_PIPELINES` integration type and appear in the BigQuery **Lineage** tab alongside runs from Dataform in Google Cloud.

---

## Prerequisites

1. **Enable the Data Lineage API** in the Google Cloud project(s) where your Dataform actions execute:

   ```bash
   gcloud services enable datalineage.googleapis.com --project=YOUR_GCP_PROJECT_ID
   ```

2. **Grant IAM permissions** to the identity configured in `.df-credentials.json` (your Application Default Credentials principal, service account JSON key, or `impersonateServiceAccount` target):
   - Predefined role: `roles/datalineage.producer` (**Data Lineage Events Producer**) on the target project(s), or
   - Custom role containing `datalineage.locations.processOpenLineageMessage`.

---

## Enabling and Disabling Lineage Emission

You can configure lineage emission at the project level in `workflow_settings.yaml` or per invocation with the `--emit-lineage` CLI flag.

### 1. Project Configuration (`workflow_settings.yaml`)

Add the `lineage` block to `workflow_settings.yaml` (see [`LineageSettings`](reference/configs.md#dataform-LineageSettings)):

```yaml
defaultProject: my-gcp-project
defaultLocation: US
defaultDataset: dataform
defaultAssertionDataset: dataform_assertions
lineage:
  enabled: true
```

### 2. CLI Flag (`--emit-lineage` / `--no-emit-lineage`)

Enable or disable lineage emission for a single run:

```bash
# Enable lineage emission for this run
dataform run --emit-lineage

# Disable lineage emission for this run (overrides lineage.enabled: true in workflow_settings.yaml)
dataform run --no-emit-lineage
```

### Precedence Rules

The CLI resolves whether to emit lineage in the following order:

| `--emit-lineage` CLI flag | `workflow_settings.yaml` (`lineage.enabled`) | `--dry-run` | Effective Behavior | `stderr` Output when Skipped |
| :--- | :--- | :--- | :--- | :--- |
| `false` (`--no-emit-lineage` or `--emit-lineage=false`) | Any | Any | **Disabled** | `[lineage] Skipped lineage emission: skip_reason=invocation_override (--emit-lineage=false)` |
| `true` (`--emit-lineage`) | Any | `false` | **Enabled** | — |
| Unset | `false` | Any | **Disabled** | `[lineage] Skipped lineage emission: skip_reason=workflow_opt_out (workflow_settings.yaml lineage.enabled=false)` |
| Unset | `true` | `false` | **Enabled** | — |
| Unset | Unset | Any | **Disabled** (default) | *(silent)* |
| `true` or Unset (with `lineage.enabled: true`) | Any | `true` | **Skipped** | `[lineage] Skipped lineage emission (dry-run mode; once-per-run): skip_reason=dry_run` |

---

## Supported Action Types and Payload Semantics

Lineage events are emitted for every Dataform action that executes a BigQuery job. Actions that do not execute a BigQuery job (such as `declaration` and `test`) do not emit lineage events.

| Dataform Action Type | `gcp_bq_pipelines_job.actionType` | Emits `START` / Terminal `RunEvent`s | Populates `inputs` and `outputs` on `COMPLETE` | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `table`, `view`, `incremental` | `TABLE` | Yes | Yes | `inputs` are populated from `dependencyTargets`; `outputs` is the 3-part `<database>.<schema>.<name>` target. |
| `assertion` (manual & built-in) | `ASSERTION` | Yes | Yes | `outputs` is the assertion view written to the assertion dataset; `inputs` are the assertion's `dependencyTargets`. |
| `propertyGraph` | `PROPERTY_GRAPH` | Yes | Yes | `inputs` are populated from `dependencyTargets` (node/edge tables referenced via `ref`). |
| `operations` (`hasOutput: true`) | `OPERATIONS` | Yes | Yes | Declares a referenceable output dataset at `target`, so both `inputs` and `outputs` are populated on `COMPLETE`. |
| `operations` (`hasOutput: false` / unset) | `OPERATIONS` | Yes | No (`[]`) | `Process` and `Run` events are recorded in Data Lineage, but `inputs` and `outputs` are empty because no single output table is declared at `target`. |
| `declaration` | — | No | No | Source declarations do not run a BigQuery job; they appear as upstream `inputs` on dependent actions. |

`inputs` and `outputs` are populated only on `COMPLETE` events (`START`, `FAIL`, and `ABORT` events carry empty `inputs` and `outputs` arrays) so failed or cancelled runs do not create false lineage edges.

---

## Where Lineage Appears in Google Cloud

For each executed action, the CLI sends OpenLineage `RunEvent`s to `projects/<projectId>/locations/<location>`, where:

- `<projectId>` is `action.target.database` (falling back to `projectId` from `.df-credentials.json`).
- `<location>` is the lowercased BigQuery location from `.df-credentials.json` (for example, `us`, `eu`, or `us-central1`).

In the Google Cloud console (**BigQuery Studio** → select a table or view → **Lineage** tab):

- **Integration / Source Type:** `BIGQUERY_PIPELINES`
- **Process Name:** `<projectId>.<location>.cli.<workdir-slug-hash>.<schema>.<name>` (stable across repeated runs from the same project directory)
- **Parent Run:** All actions within a single `dataform run` invocation share a parent run (`<projectId>.<location>.cli.<workdir-slug-hash>.run`) and report `gcp_bq_pipelines_run.runType = "cli-manual"`.
- **BigQuery Job Link (`externalQuery`):** Terminal events include the `externalQuery` run facet (`<credentialsProjectId>.<location>.<bqJobId>`) linking the lineage run to the underlying BigQuery job.

---

## Endpoint Routing and Failure Semantics

Lineage emission is **fail-open**: lineage errors are logged to `stderr` and **never** cause `dataform run` to fail or exit with a non-zero status code.

- **Regional Endpoint (REP) Routing with Global Fallback:** The emitter first sends requests to the regional Data Lineage endpoint `datalineage.<location>.rep.googleapis.com`. If the regional endpoint is not resolvable via DNS or returns an HTTP 301/302 redirect (indicating no regional route for that location), the CLI logs a notice to `stderr` and automatically falls back to `datalineage.googleapis.com` for that location for the remainder of the run.
- **Automatic Retries:** Transient gRPC status codes (`DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`, `INTERNAL`, `UNAVAILABLE`) are retried automatically via `google-gax` with exponential backoff (`1s`, `2s`, `4s`; `2s` per-attempt RPC timeout; `15s` total retry budget).
- **Run-Wide Circuit Breaker on Non-Recoverable Setup Errors:** If an action hits `PERMISSION_DENIED` (`7`), `FAILED_PRECONDITION` / `SERVICE_DISABLED` (`9`), `UNAUTHENTICATED` (`16`), or an unrecoverable `endpoint_region_mismatch`, the emitter writes a single `[lineage] Skipped lineage emission for the rest of this run: skip_reason=...` warning to `stderr` and disables further lineage RPCs for the rest of that invocation.
- **Drain on Exit:** Before `dataform run` exits, the CLI waits up to `15s` for any in-flight lineage RPCs to settle.

---

## Troubleshooting

### Verbose Debug Logging

Set `DATAFORM_LINEAGE_DEBUG=1` to print every OpenLineage payload and emission result to `stderr`:

```bash
DATAFORM_LINEAGE_DEBUG=1 dataform run --emit-lineage
```

When enabled, the CLI logs:

- `[lineage-debug] emit endpoint=<host> location=<loc> action=<schema>.<name> eventType=<START|COMPLETE|FAIL|ABORT> runId=<uuid> parentRunId=<uuid> payload=<json>`
- `[lineage-debug] emit_ok action=<schema>.<name> eventType=<START|COMPLETE|FAIL|ABORT>`

### Common `stderr` Messages and `skip_reason` Values

| `skip_reason` / Message | Cause | Resolution |
| :--- | :--- | :--- |
| `skip_reason=api_disabled` (`PERMISSION_DENIED` or `SERVICE_DISABLED`) | Either `datalineage.googleapis.com` is not enabled in the target project, or the execution identity lacks `datalineage.locations.processOpenLineageMessage`. | Run `gcloud services enable datalineage.googleapis.com --project=<PROJECT_ID>` and grant `roles/datalineage.producer` to the principal in `.df-credentials.json`. |
| `skip_reason=unauthenticated` (`UNAUTHENTICATED`) | Credentials in `.df-credentials.json` or Application Default Credentials are missing, invalid, or expired. | Re-authenticate (for example, `gcloud auth application-default login`) or refresh `.df-credentials.json` via `dataform init-creds`. |
| `skip_reason=invocation_override` | `--no-emit-lineage` or `--emit-lineage=false` was passed on the command line. | Run `dataform run --emit-lineage` or omit `--no-emit-lineage` when `lineage.enabled: true` is set in `workflow_settings.yaml`. |
| `skip_reason=workflow_opt_out` | `lineage.enabled: false` is set in `workflow_settings.yaml` and `--emit-lineage` was not passed. | Set `lineage.enabled: true` in `workflow_settings.yaml` or pass `--emit-lineage`. |
| `skip_reason=dry_run` | `dataform run --dry-run` was used. | Remove `--dry-run` to execute BigQuery jobs and emit lineage events. |
| `skip_reason=endpoint_region_mismatch` | Both the regional and fallback endpoints returned HTTP 301/302 for the configured `location`. | Verify that the `location` in `.df-credentials.json` / `workflow_settings.yaml` is a supported BigQuery and Data Lineage location. |
| *(No lineage logs at all)* | Neither `--emit-lineage` nor `workflow_settings.yaml` `lineage.enabled` is set, or the run only executed `declaration` actions. | Pass `--emit-lineage` or set `lineage.enabled: true` in `workflow_settings.yaml`, and run with `DATAFORM_LINEAGE_DEBUG=1` to inspect emitted events. |
