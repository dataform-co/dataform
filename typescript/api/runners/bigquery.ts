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

  tables() {
    return this.client
      .getDatasets()
      .then(datasets =>
        Promise.all(
          datasets.map(dataset => dataset.getTables({ autoPaginate: true }))
        )
      ).then(datasetTables => {
        var allTables = [];
        datasetTables.forEach(tables => tables.forEach(table => allTables.push(`${dataset.name}.${table.name}`)));
      });
  }

  schema(table: string) {}
}
