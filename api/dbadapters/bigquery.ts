import { Credentials } from "@dataform/api/commands/credentials";
import { adapters } from "@dataform/core";
import { IDbAdapter, OnCancel } from "@dataform/api/dbadapters/index";
import { parseBigqueryEvalError } from "@dataform/api/utils/error_parsing";
import { dataform } from "@dataform/protos";
import { BigQuery } from "@google-cloud/bigquery";
import { QueryResultsOptions } from "@google-cloud/bigquery/build/src/job";
import * as Long from "long";
import * as PromisePool from "promise-pool-executor";

import { STATE_PERSIST_TABLE_NAME } from "@dataform/api/dbadapters/index";

const EXTRA_GOOGLE_SCOPES = ["https://www.googleapis.com/auth/drive"];

const BIGQUERY_DATE_RELATED_FIELDS = [
  "BigQueryDate",
  "BigQueryTime",
  "BigQueryTimestamp",
  "BigQueryDatetime"
];

interface IBigQueryTableMetadata {
  type: string;
  schema: {
    fields: IBigQueryFieldMetadata[];
  };
  tableReference: {
    projectId: string;
    datasetId: string;
    tableId: string;
  };
  lastModifiedTime: string;
}

interface IBigQueryFieldMetadata {
  name: string;
  mode: string;
  type: string;
  fields?: IBigQueryFieldMetadata[];
}

export class BigQueryDbAdapter implements IDbAdapter {
  private bigQueryCredentials: dataform.IBigQuery;
  private pool: PromisePool.PromisePoolExecutor;

  private readonly clients = new Map<string, BigQuery>();

  constructor(credentials: Credentials) {
    this.bigQueryCredentials = credentials as dataform.IBigQuery;
    // Bigquery allows 50 concurrent queries, and a rate limit of 100/user/second by default.
    // These limits should be safely low enough for most projects.
    this.pool = new PromisePool.PromisePoolExecutor({
      concurrencyLimit: 16,
      frequencyWindow: 1000,
      frequencyLimit: 30
    });
  }

  public async execute(
    statement: string,
    options: {
      onCancel?: OnCancel;
      interactive?: boolean;
      maxResults?: number;
    } = { interactive: false, maxResults: 1000 }
  ) {
    return this.pool
      .addSingleTask({
        generator: () =>
          options && options.interactive
            ? this.runQuery(statement, options && options.maxResults)
            : this.createQueryJob(
                statement,
                options && options.maxResults,
                options && options.onCancel
              )
      })
      .promise();
  }

  public async evaluate(statement: string) {
    try {
      await this.getClient().query({
        useLegacySql: false,
        query: statement,
        dryRun: true
      });
      return dataform.QueryEvaluationResponse.create({
        status: dataform.QueryEvaluationResponse.QueryEvaluationStatus.SUCCESS
      });
    } catch (e) {
      return dataform.QueryEvaluationResponse.create({
        status: dataform.QueryEvaluationResponse.QueryEvaluationStatus.FAILURE,
        error: parseBigqueryEvalError(e)
      });
    }
  }

  public tables(): Promise<dataform.ITarget[]> {
    return this.getClient()
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

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const metadata = await this.getMetadata(target);

    if (!metadata) {
      return null;
    }

    return dataform.TableMetadata.create({
      type: String(metadata.type).toLowerCase(),
      target,
      fields: metadata.schema.fields.map(field => convertField(field)),
      lastUpdatedMillis: Long.fromString(metadata.lastModifiedTime)
    });
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const metadata = await this.getMetadata(target);
    if (metadata.type === "TABLE") {
      // For tables, we use the BigQuery tabledata.list API, as per https://cloud.google.com/bigquery/docs/best-practices-costs#preview-data.
      // Also see https://cloud.google.com/nodejs/docs/reference/bigquery/3.0.x/Table#getRows.
      const rowsResult = await this.pool
        .addSingleTask({
          generator: () =>
            this.getClient(target.database)
              .dataset(target.schema)
              .table(target.name)
              .getRows({
                maxResults: limitRows
              })
        })
        .promise();
      return cleanRows(rowsResult[0]);
    }
    const { rows } = await this.runQuery(
      `SELECT * FROM \`${metadata.tableReference.projectId}.${metadata.tableReference.datasetId}.${metadata.tableReference.tableId}\``,
      limitRows
    );
    return rows;
  }

  public async prepareSchema(database: string, schema: string): Promise<void> {
    const location = this.bigQueryCredentials.location || "US";
    const client = this.getClient(database);

    let metadata;
    try {
      const data = await client.dataset(schema).getMetadata();
      metadata = data[0];
    } catch (e) {
      // If metadata call fails, it probably doesn't exist. So try to create it.
      await client.createDataset(schema, { location });
      return;
    }

    if (metadata.location.toUpperCase() !== location.toUpperCase()) {
      throw new Error(
        `Cannot create dataset "${schema}" in location "${location}". It already exists in location "${metadata.location}". Change your default dataset location or delete the existing dataset.`
      );
    }
  }

  public async dropSchema(database: string, schema: string): Promise<void> {
    const client = this.getClient(database);
    try {
      await client.dataset(schema).getMetadata();
    } catch (e) {
      // If metadata call fails, it probably doesn't exist, so don't do anything.
      return;
    }
    await client.dataset(schema).delete({ force: true });
  }

  public async close() {}

  public async persistedStateMetadata(
    compiledGraph: dataform.ICompiledGraph
  ): Promise<dataform.IPersistedTableMetadata[]> {
    const adapter = adapters.create(
      compiledGraph.projectConfig,
      compiledGraph.dataformCoreVersion || "1.0.0"
    );
    try {
      const { rows } = await this.runQuery(
        `SELECT * FROM \`${adapter.resolveTarget({ name: STATE_PERSIST_TABLE_NAME })}\``,
        5000 // not expecting to have more dataset than this
      );
      const peristedMetadata = rows.map(row => {
        const encodedProto = Buffer.from(row.proto, "base64");
        return dataform.PersistedTableMetadata.decode(encodedProto);
      });
      return peristedMetadata || [];
    } catch (err) {
      return [];
    }
  }

  private getClient(projectId?: string) {
    projectId = projectId || this.bigQueryCredentials.projectId;
    if (!this.clients.has(projectId)) {
      this.clients.set(
        projectId,
        new BigQuery({
          projectId,
          credentials: JSON.parse(this.bigQueryCredentials.credentials),
          scopes: EXTRA_GOOGLE_SCOPES
        })
      );
    }
    return this.clients.get(projectId);
  }

  private async runQuery(statement: string, maxResults?: number) {
    const results = await new Promise<any[]>((resolve, reject) => {
      const allRows: any[] = [];
      const stream = this.getClient().createQueryStream(statement);
      stream
        .on("error", reject)
        .on("data", row => {
          if (!maxResults) {
            allRows.push(row);
          } else if (allRows.length < maxResults) {
            allRows.push(row);
          } else {
            stream.end();
          }
        })
        .on("end", () => {
          resolve(allRows);
        });
    });
    return { rows: cleanRows(results), metadata: {} };
  }

  private createQueryJob(statement: string, maxResults?: number, onCancel?: OnCancel) {
    let isCancelled = false;
    if (onCancel) {
      onCancel(() => {
        isCancelled = true;
      });
    }

    return new Promise<any>((resolve, reject) =>
      this.getClient().createQueryJob(
        { useLegacySql: false, jobPrefix: "dataform-", query: statement, maxResults },
        async (err, job) => {
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

          let results: any[] = [];
          const manualPaginationCallback = (
            e: Error,
            rows: any[],
            nextQuery: QueryResultsOptions
          ) => {
            if (e) {
              reject(e);
              return;
            }
            results = results.concat(rows.slice(0, maxResults - results.length));
            if (nextQuery && results.length < maxResults) {
              // More results exist and we have space to consume them.
              job.getQueryResults(nextQuery, manualPaginationCallback);
            } else {
              job.getMetadata().then(([bqMeta]) => {
                const queryData = {
                  rows: results,
                  metadata: {
                    bigquery: {
                      jobId: bqMeta.jobReference.jobId,
                      totalBytesBilled: Long.fromString(bqMeta.statistics.query.totalBytesBilled),
                      totalBytesProcessed: Long.fromString(
                        bqMeta.statistics.query.totalBytesProcessed
                      )
                    }
                  }
                };
                resolve(queryData);
              });
            }
          };
          // For non interactive queries, we can set a hard limit by disabling auto pagination.
          // This will cause problems for unit tests that have more than MAX_RESULTS rows to compare.
          job.getQueryResults({ autoPaginate: false, maxResults }, manualPaginationCallback);
        }
      )
    );
  }

  private async getMetadata(target: dataform.ITarget): Promise<IBigQueryTableMetadata> {
    const metadataResult = await this.pool
      .addSingleTask({
        generator: async () => {
          try {
            const table = await this.getClient(target.database)
              .dataset(target.schema)
              .table(target.name)
              .getMetadata();
            return table;
          } catch (e) {
            if (e && e.errors && e.errors[0] && e.errors[0].reason === "notFound") {
              // if the table can't be found, just return null
              return null;
            }
            // otherwise throw the error as normal
            throw e;
          }
        }
      })
      .promise();
    return metadataResult && metadataResult[0];
  }
}

function cleanRows(rows: any[]) {
  if (rows.length === 0) {
    return rows;
  }

  const sampleData = rows[0];
  const fieldsWithBigQueryDates = Object.keys(sampleData).filter(
    key =>
      sampleData[key] &&
      sampleData[key].constructor &&
      BIGQUERY_DATE_RELATED_FIELDS.includes(sampleData[key].constructor.name)
  );
  fieldsWithBigQueryDates.forEach(dateField => {
    rows.forEach(row => (row[dateField] = row[dateField] ? row[dateField].value : row[dateField]));
  });
  return rows;
}

function convertField(field: IBigQueryFieldMetadata): dataform.IField {
  const result: dataform.IField = {
    name: field.name,
    flags: !!field.mode ? [field.mode] : []
  };
  if (field.type === "RECORD") {
    result.struct = { fields: field.fields.map(innerField => convertField(innerField)) };
  } else {
    result.primitive = field.type;
  }
  return result;
}
