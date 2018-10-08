import { Runner } from "./index";
import * as protos from "@dataform/protos";
const BigQuery = require("@google-cloud/bigquery");

export class BigQueryRunner implements Runner {
  private profile: protos.IProfile;
  private client: any;

  constructor(profile: protos.IProfile) {
    this.profile = profile;
    this.client = BigQuery(profile.bigquery.projectId, {
      credentials: profile.bigquery.credentials
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
          tablesResult[0].forEach(table =>
            allTables.push({ schema: table.dataset.id, name: table.id })
          )
        );
        return allTables;
      });
  }

  schema(target: protos.ITarget): Promise<protos.ITable> {
    return this.client
      .dataset(target.schema)
      .table(target.name)
      .getMetadata()
      .then(result => {
        var table = result[0];
        return protos.Table.create({
          target: target,
          schema: {
            fields: table.schema.fields.map(field => convertField(field))
          }
        });
      });
  }
}

function convertField(field: any): protos.Schema.IField {
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
