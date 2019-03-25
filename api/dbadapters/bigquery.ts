import * as protos from "@dataform/protos";
import * as PromisePool from "promise-pool-executor";
import { DbAdapter, OnCancel } from "./index";

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

  public execute(statement: string, onCancel?: OnCancel) {
    let isCancelled = false;
    onCancel &&
      onCancel(() => {
        isCancelled = true;
      });
    return this.pool
      .addSingleTask({
        generator: () =>
          new Promise<any[]>((resolve, reject) => {
            this.client.createQueryJob(
              { useLegacySql: false, query: statement, maxResults: 1000 },
              async (err: any, job: any) => {
                if (err) {
                  return reject(err);
                }
                // Cancelled before it was created, kill it now.
                if (isCancelled) {
                  await job.cancel();
                  return reject(new Error("Query cancelled."));
                }
                onCancel &&
                  onCancel(async () => {
                    // Cancelled while running.
                    await job.cancel();
                    return reject(new Error("Query cancelled."));
                  });
                job.getQueryResults((err: any, result: any[]) => {
                  if (err) {
                    reject(err);
                  }
                  resolve(result);
                });
              }
            );
          })
      })
      .promise();
  }

  public evaluate(statement: string) {
    return this.client.query({
      useLegacySql: false,
      query: statement,
      dryRun: true
    });
  }

  public tables(): Promise<protos.ITarget[]> {
    return this.client
      .getDatasets({ autoPaginate: true })
      .then((result: any) => {
        return Promise.all(
          result[0].map((dataset: any) => {
            return this.pool
              .addSingleTask({
                generator: () => dataset.getTables({ autoPaginate: true })
              })
              .promise();
          })
        );
      })
      .then((datasetTables: any) => {
        const allTables: protos.ITarget[] = [];
        datasetTables.forEach((tablesResult: any) =>
          tablesResult[0].forEach((table: any) =>
            allTables.push({ schema: table.dataset.id, name: table.id })
          )
        );
        return allTables;
      });
  }

  public table(target: protos.ITarget): Promise<protos.ITableMetadata> {
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
        const table = result[0];
        return protos.TableMetadata.create({
          type: String(table.type).toLowerCase(),
          target,
          fields: table.schema.fields.map(field => convertField(field))
        });
      });
  }

  public prepareSchema(schema: string): Promise<void> {
    const location = this.profile.bigquery.location || "US";

    // If metadata call fails, it probably doesn't exist. So try to create it.
    return this.client
      .dataset(schema)
      .getMetadata()
      .then(
        metadata => {
          // check location of the dataset
          const wrongMetadata = metadata.find(
            md => md.location.toUpperCase() !== location.toUpperCase()
          );
          if (wrongMetadata) {
            const message = `Cannot create dataset "${schema}" in location "${location}" as it already exists in location "${
              wrongMetadata.location
            }". Change your default dataset location or delete the existing dataset.`;
            throw Error(message);
          }
        },
        _ => this.client.createDataset(schema, { location }).then(() => {})
      )
      .catch(e => console.error(e));
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
