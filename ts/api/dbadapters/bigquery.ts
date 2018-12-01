import { DbAdapter } from "./index";
import * as protos from "@dataform/protos";

const BigQuery = require("@google-cloud/bigquery");

export class BigQueryDbAdapter implements DbAdapter {
  private profile: protos.IProfile;
  private client: any;

  constructor(profile: protos.IProfile) {
    this.profile = profile;
    this.client = BigQuery({
      projectId: profile.bigquery.projectId,
      credentials: JSON.parse(profile.bigquery.credentials)
    });
  }

  execute(statement: string) {
    return this.client
      .query({
        useLegacySql: false,
        query: statement
      })
      .then(result => result[0]);
  }

  evaluate(statement: string) {
    return this.client
      .query({
        useLegacySql: false,
        query: statement,
        dryRun: true
      });
  }

  tables(): Promise<protos.ITarget[]> {
    return this.client
      .getDatasets({ autoPaginate: true })
      .then(result => {
        return Promise.all(
          result[0].map(dataset => {
            return dataset.getTables({ autoPaginate: true });
          })
        );
      })
      .then(datasetTables => {
        var allTables: protos.ITarget[] = [];
        datasetTables.forEach(tablesResult =>
          tablesResult[0].forEach(table => allTables.push({ schema: table.dataset.id, name: table.id }))
        );
        return allTables;
      });
  }

  table(target: protos.ITarget): Promise<protos.ITable> {
    return this.client
      .dataset(target.schema)
      .table(target.name)
      .getMetadata()
      .then(result => {
        var table = result[0];
        return protos.Table.create({
          type: String(table.type).toLowerCase(),
          target: target,
          fields: table.schema.fields.map(field => convertField(field))
        });
      });
  }

  prepareSchema(schema: string): Promise<void> {
    // If metadata call fails, it probably doesn't exist. So try to create it.
    return this.client
      .dataset(schema)
      .getMetadata()
      .catch(_ =>
        this.client
          .createDataset(schema, {
            location: this.profile.bigquery.location || "US"
          })
          .then(() => {})
      );
  }
}

function convertField(field: any): protos.IField {
  return {
    name: field.name,
    flags: [field.mode],
    primitive: field.type != "RECORD" ? field.type : null,
    struct:
      field.type == "RECORD"
        ? {
            fields: field.fields.map(field => convertField(field))
          }
        : null
  };
}
