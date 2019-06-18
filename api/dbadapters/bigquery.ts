import { Credentials } from "@dataform/api/commands/credentials";
import { DbAdapter, OnCancel } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";
import { BigQuery } from "@google-cloud/bigquery";
import * as PromisePool from "promise-pool-executor";

export class BigQueryDbAdapter implements DbAdapter {
  private bigQueryCredentials: dataform.IBigQuery;
  private client: any;
  private pool: PromisePool.PromisePoolExecutor;

  constructor(credentials: Credentials) {
    this.bigQueryCredentials = credentials as dataform.IBigQuery;
    this.client = new BigQuery({
      projectId: this.bigQueryCredentials.projectId,
      credentials: JSON.parse(this.bigQueryCredentials.credentials),
      scopes: ["https://www.googleapis.com/auth/drive"]
    });
    // Bigquery allows 50 concurrent queries, and a rate limit of 100/user/second by default.
    // These limits should be safely low enough for most projects.
    this.pool = new PromisePool.PromisePoolExecutor({
      concurrencyLimit: 16,
      frequencyWindow: 1000,
      frequencyLimit: 30
    });
  }

  public execute(statement: string, onCancel?: OnCancel) {
    let isCancelled = false;
    if (onCancel) {
      onCancel(() => {
        isCancelled = true;
      });
    }
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
                if (onCancel) {
                  onCancel(async () => {
                    // Cancelled while running.
                    await job.cancel();
                    return reject(new Error("Query cancelled."));
                  });
                }
                job.getQueryResults((err: any, result: any[]) => {
                  if (err) {
                    reject(err);
                  }
                  resolve(this.cleanRows(result));
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

  public tables(): Promise<dataform.ITarget[]> {
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
        const allTables: dataform.ITarget[] = [];
        datasetTables.forEach((tablesResult: any) =>
          tablesResult[0].forEach((table: any) =>
            allTables.push({ schema: table.dataset.id, name: table.id })
          )
        );
        return allTables;
      });
  }

  public table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
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
        return dataform.TableMetadata.create({
          type: String(table.type).toLowerCase(),
          target,
          fields: table.schema.fields.map(field => convertField(field))
        });
      });
  }

  public async prepareSchema(schema: string): Promise<void> {
    const location = this.bigQueryCredentials.location || "US";

    let metadata;
    try {
      const data = await this.client.dataset(schema).getMetadata();
      metadata = data[0];
    } catch (e) {
      // If metadata call fails, it probably doesn't exist. So try to create it.
      return await this.client.createDataset(schema, { location });
    }

    if (metadata.location.toUpperCase() !== location.toUpperCase()) {
      throw new Error(
        `Cannot create dataset "${schema}" in location "${location}". It already exists in location "${metadata.location}". Change your default dataset location or delete the existing dataset.`
      );
    }
  }

  private cleanRows(rows: any[]) {
    if (rows.length === 0) {
      return rows;
    }

    const sampleData = rows[0];
    const BIGQUERY_DATE_CLASS_NAME = "BigQueryDate";
    const fieldsWithBigQueryDates = Object.keys(sampleData).filter(
      key => sampleData[key].constructor.name === BIGQUERY_DATE_CLASS_NAME
    );
    if (fieldsWithBigQueryDates.length === 0) {
      return rows;
    } else {
      const reformattedResults = rows.map(row => {
        const newRow = { ...row };
        fieldsWithBigQueryDates.forEach(field => {
          newRow[field] = newRow[field].value;
        });
        return newRow;
      });
      return reformattedResults;
    }
  }
}

function convertField(field: any): dataform.IField {
  const result: dataform.IField = {
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
