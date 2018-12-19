import { Props as StructProps } from "./components/struct";

export const structs: { [name: string]: StructProps } = {
  dataformJson: {
    description: "The `dataform.json` file contains project level information.",
    fields: [
      {
        name: "warehouse",
        type: '"bigquery" | "redshift"',
        description: "The type of the warehouse to compile against"
      },
      {
        name: "defaultSchema",
        type: "string",
        description: "The default output schema/dataset for materializations"
      },
      {
        name: "assertionsSchema",
        type: "string",
        description: "The default output schema/dataset for assertions"
      },
      {
        name: "gcloudProjectId",
        type: "string",
        description: "The Google cloud project ID for BigQuery"
      }
    ]
  },
  materializationConfig: {
    description:
      "The `MaterializationConfig` interface can be used to set several properties of a materialization at once.",
    fields: [
      {
        name: "type",
        type: '"view" | "table" | "incremental"',
        description: "The type of the materialization"
      },
      {
        name: "query",
        type: "Contextable<string>",
        description: "The materialization select query"
      },
      {
        name: "where",
        type: "Contextable<string>",
        description: "The where clause for incremental tables"
      },
      {
        name: "preOps",
        type: "Contextable<string | string[]>",
        description: "Operations to run before the materialization"
      },
      {
        name: "postOps",
        type: "Contextable<string | string[]>",
        description: "Operations to run after the materialization"
      },
      {
        name: "dependencies",
        type: "string | string[]",
        description: "The dependencies of this node"
      },
      {
        name: "descriptor",
        type: "{ [field: string]: string }",
        description: "The descriptor for the table, a map of field names to descriptions"
      }
    ]
  }
};
