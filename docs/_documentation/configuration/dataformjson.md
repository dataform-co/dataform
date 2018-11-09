The `dataform.json` file stores project level information, such as the type of warehouse the project should compile against, or the default schemas/datasets to use.

An example `dataform.json` file:

```json
{
  "warehouse": "bigquery",
  "defaultSchema": "dataform_output",
  "assertionSchema": "dataform_assertions",
  "projectId": "gcloud-project-id"

}
```

### `warehouse`

The `warehouse` setting specifies the type of warehouse the project will run against, and can be one of `["bigquery", "redshift", "snowflake", "postgres"]`.

### `defaultSchema`

The `defaultSchema` setting refers to the schema (or dataset in BigQuery) in your warehouse that materializations will be written to.

### `assertionSchema`

The `assertionSchema` setting refers to the schema (or dataset in BigQuery) in your warehouse that assertions will be written to.

###  `gcloudProjectId` (BigQuery)

For BigQuery you must set the `gcloudProjectId` field to be your Google Cloud Project ID.
