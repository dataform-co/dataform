import { DbAdapter } from "./index";
import * as protos from "@dataform/protos";
import * as PromisePool from "promise-pool-executor";
import * as Promise from "bluebird";
import * as EventEmitter from "events";
const BigQuery = require("@google-cloud/bigquery");

export class BigQueryDbAdapter implements DbAdapter {
  private profile: protos.IProfile;
  private client: any;
  private pool: PromisePool.PromisePoolExecutor;

  constructor(profile: protos.IProfile) {
    this.profile = profile;
    this.client = BigQuery({
      projectId: profile.bigquery.projectId,
      credentials: JSON.parse(profile.bigquery.credentials),
      scopes: ["https://www.googleapis.com/auth/drive"]
    });
    // Bigquery allows 50 concurrent queries, and a rate limit of 100/user/second by default.
    // These limits should be safely low enough for most projects.
    this.pool = new PromisePool.PromisePoolExecutor({
      concurrencyLimit: this.profile.threads || 16,
      frequencyWindow: 1000,
      frequencyLimit: 30
    });
  }

  execute(statement: string) {
    this.pool.addSingleTask({
      generator: () =>
        new Promise((resolve, reject, onCancel) => {
          let isCanceled = false;
          const eEmitter = new EventEmitter();

          onCancel(() => {
            isCanceled = true;
            eEmitter.emit("jobCancel");
          });

          this.client.createQueryJob({ useLegacySql: false, query: statement, maxResults: 1000 }, (err, job) => {
            if (err) reject(err);

            eEmitter.on("jobCancel", () => {
              job.cancel().then(() => {
                reject(new Error("Run cancelled"));
              });
            });

            if (isCanceled) {
              return;
            }
            job.getQueryResults((err, result) => {
              if (err) reject(err);
              resolve(result);
            });
          });
        })
    }).promise();
  }

  evaluate(statement: string) {
    return this.client.query({
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
            return this.pool
              .addSingleTask({
                generator: () => dataset.getTables({ autoPaginate: true })
              })
              .promise();
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

  table(target: protos.ITarget): Promise<protos.ITableMetadata> {
    return this.pool
      .addSingleTask({
        generator: () =>
          this.client
            .dataset(target.schema)
            .table(target.name)
            .getMetadata()
      })
      .promise()
      .then(result => {
        var table = result[0];
        return protos.TableMetadata.create({
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
  const result: protos.IField = {
    name: field.name,
    flags: !!field.mode ? [field.mode] : []
  };
  if (field.type == "RECORD") {
    result.struct = { fields: field.fields.map(field => convertField(field)) };
  } else {
    result.primitive = field.type;
  }
  return result;
}
