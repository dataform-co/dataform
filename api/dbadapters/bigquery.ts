import Long from "long";
import { PromisePoolExecutor } from "promise-pool-executor";

import { BigQuery } from "@google-cloud/bigquery";
import { QueryResultsOptions } from "@google-cloud/bigquery/build/src/job";
import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IExecutionResult, OnCancel } from "df/api/dbadapters/index";
import { parseBigqueryEvalError } from "df/api/utils/error_parsing";
import {
  buildQuery,
  decodePersistedTableMetadata,
  hashExecutionAction,
  IMetadataRow
} from "df/api/utils/run_cache";
import { ErrorWithCause } from "df/common/errors/errors";
import {
  JSONObjectStringifier,
  StringifiedMap,
  StringifiedSet
} from "df/common/strings/stringifier";
import { dataform } from "df/protos/ts";

const CACHED_STATE_TABLE_NAME = "dataform_meta.cache_state";

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
  description?: string;
}

interface IBigQueryFieldMetadata {
  name: string;
  mode: string;
  type: string;
  fields?: IBigQueryFieldMetadata[];
  description?: string;
}

export class BigQueryDbAdapter implements IDbAdapter {
  public static async create(credentials: Credentials) {
    return new BigQueryDbAdapter(credentials);
  }

  private bigQueryCredentials: dataform.IBigQuery;
  private pool: PromisePoolExecutor;

  private readonly clients = new Map<string, BigQuery>();

  private constructor(credentials: Credentials) {
    this.bigQueryCredentials = credentials as dataform.IBigQuery;
    // Bigquery allows 50 concurrent queries, and a rate limit of 100/user/second by default.
    // These limits should be safely low enough for most projects.
    this.pool = new PromisePoolExecutor({
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
  ): Promise<IExecutionResult> {
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
      lastUpdatedMillis: Long.fromString(metadata.lastModifiedTime),
      description: metadata.description
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

  public async close() {
    // Unimplemented.
  }

  public async prepareStateMetadataTable(): Promise<void> {
    const metadataTableCreateQuery = `
      CREATE TABLE IF NOT EXISTS \`${CACHED_STATE_TABLE_NAME}\` (
        target_name STRING,
        metadata_json STRING,
        metadata_proto STRING
      )
    `;
    await this.runQuery(metadataTableCreateQuery);
  }

  public async persistedStateMetadata(): Promise<dataform.IPersistedTableMetadata[]> {
    const { rows } = await this.runQuery(
      `SELECT * FROM ${CACHED_STATE_TABLE_NAME}`,
      5000 // TODO: Add pagination for 5000+ rows
    );
    const persistedMetadata = rows.map((row: IMetadataRow) =>
      decodePersistedTableMetadata(row.metadata_proto)
    );
    return persistedMetadata;
  }

  public async persistStateMetadata(actions: dataform.IExecutionAction[]): Promise<void> {
    if (actions.length === 0) {
      return;
    }
    const allInvolvedTargets = new StringifiedSet(JSONObjectStringifier.create<dataform.ITarget>());
    actions.forEach(action => {
      allInvolvedTargets.add(action.target);
      action.transitiveInputs.forEach(transitiveInput => allInvolvedTargets.add(transitiveInput));
    });

    const tableMetadataByTarget = new StringifiedMap<dataform.ITarget, dataform.ITableMetadata>(
      JSONObjectStringifier.create()
    );
    await Promise.all(
      Array.from(allInvolvedTargets).map(async target => {
        tableMetadataByTarget.set(target, await this.table(target));
      })
    );
    const queries = actions.map(action => {
      const persistTable = dataform.PersistedTableMetadata.create({
        target: action.target,
        lastUpdatedMillis: tableMetadataByTarget.get(action.target).lastUpdatedMillis,
        definitionHash: hashExecutionAction(action),
        transitiveInputTables: action.transitiveInputs.map(transitiveInput => {
          if (!tableMetadataByTarget.has(transitiveInput)) {
            throw new Error(`Could not find table metadata for ${JSON.stringify(transitiveInput)}`);
          }
          return tableMetadataByTarget.get(transitiveInput);
        })
      });

      const targetName = `${action.target.database}.${action.target.schema}.${action.target.name}`;

      return buildQuery(targetName, persistTable);
    });

    const unionQuery = queries.join(" UNION ALL ");

    const updateQuery = `MERGE INTO \`${CACHED_STATE_TABLE_NAME}\` T
    USING (${unionQuery}) S
    ON (T.target_name = S.target_name)
    WHEN NOT MATCHED THEN
      INSERT (target_name, metadata_json, metadata_proto)
      VALUES(S.target_name, S.metadata_json, S.metadata_proto)
    WHEN MATCHED THEN
      UPDATE SET metadata_json = S.metadata_json,
          metadata_proto = S.metadata_proto;`;
    await this.runQuery(updateQuery);
  }

  public async setMetadata(action: dataform.IExecutionAction): Promise<any> {
    const { target, actionDescriptor, type } = action;

    if (!actionDescriptor || !["view", "table"].includes(type)) {
      return;
    }

    return this.pool
      .addSingleTask({
        generator: async () => {
          const metadata = await this.getMetadataOutsidePromisePool(target);
          const schemaWithDescription = addDescriptionToMetadata(
            actionDescriptor.columns,
            metadata.schema.fields
          );

          const table = await this.getClient(target.database)
            .dataset(target.schema)
            .table(target.name)
            .setMetadata({
              description: actionDescriptor.description,
              schema: schemaWithDescription
            });
          return table;
        }
      })
      .promise();
  }

  public async getMetadata(target: dataform.ITarget): Promise<IBigQueryTableMetadata> {
    return this.pool
      .addSingleTask({
        generator: async () => this.getMetadataOutsidePromisePool(target)
      })
      .promise();
  }

  public async deleteStateMetadata(actions: dataform.IExecutionAction[]): Promise<void> {
    if (actions.length === 0) {
      return;
    }
    const targetNames = actions
      .map(({ target }) => `"${target.database}.${target.schema}.${target.name}"`)
      .join(",");
    const rowDeleteQuery = `DELETE \`${CACHED_STATE_TABLE_NAME}\` WHERE target_name IN (${targetNames})`;
    await this.runQuery(rowDeleteQuery);
  }

  private async getMetadataOutsidePromisePool(
    target: dataform.ITarget
  ): Promise<IBigQueryTableMetadata> {
    try {
      const table = await this.getClient(target.database)
        .dataset(target.schema)
        .table(target.name)
        .getMetadata();
      return table && table[0];
    } catch (e) {
      if (e && e.errors && e.errors[0] && e.errors[0].reason === "notFound") {
        // if the table can't be found, just return null
        return null;
      }
      // otherwise throw the error as normal
      throw new ErrorWithCause("Error getting BigQuery metadata.", e);
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
        .on("error", e => reject(new ErrorWithCause("Error running query." + statement, e)))
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
          try {
            if (err) {
              return reject(new ErrorWithCause("Error running query job", err));
            }
            // Cancelled before it was created, kill it now.
            if (isCancelled) {
              await job.cancel();
              return reject(new Error("Query cancelled."));
            }
            if (onCancel) {
              onCancel(async () => {
                // Cancelled while running.
                try {
                  await job.cancel();
                  reject(new Error("Query cancelled."));
                } catch (e) {
                  reject(new ErrorWithCause("Error trying to cancel query.", e));
                }
              });
            }

            let results: any[] = [];
            const manualPaginationCallback = async (
              e: Error,
              rows: any[],
              nextQuery: QueryResultsOptions
            ) => {
              try {
                if (e) {
                  reject(e);
                  return;
                }
                results = results.concat(rows.slice(0, maxResults - results.length));
                if (nextQuery && results.length < maxResults) {
                  // More results exist and we have space to consume them.
                  job.getQueryResults(nextQuery, manualPaginationCallback);
                } else {
                  const [jobMetadata] = await job.getMetadata();
                  const queryData = {
                    rows: results,
                    metadata: {
                      bigquery: {
                        jobId: jobMetadata.jobReference.jobId,
                        totalBytesBilled: Long.fromString(
                          jobMetadata.statistics.query.totalBytesBilled
                        ),
                        totalBytesProcessed: Long.fromString(
                          jobMetadata.statistics.query.totalBytesProcessed
                        )
                      }
                    }
                  };
                  resolve(queryData);
                }
              } catch (e) {
                reject(new ErrorWithCause("Error paginating query results.", e));
              }
            };
            // For non interactive queries, we can set a hard limit by disabling auto pagination.
            // This will cause problems for unit tests that have more than MAX_RESULTS rows to compare.
            job.getQueryResults({ autoPaginate: false, maxResults }, manualPaginationCallback);
          } catch (e) {
            reject(new ErrorWithCause("Error handling results of query job.", e));
          }
        }
      )
    );
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
    flags: !!field.mode ? [field.mode] : [],
    description: field.description
  };
  if (field.type === "RECORD") {
    result.struct = { fields: field.fields.map(innerField => convertField(innerField)) };
  } else {
    result.primitive = field.type;
  }
  return result;
}

function addDescriptionToMetadata(
  columnDescriptions: dataform.IColumnDescriptor[],
  metadataArray: IBigQueryFieldMetadata[]
): IBigQueryFieldMetadata[] {
  const findDescription = (path: string[]) =>
    columnDescriptions.find(column => column.path.join("") === path.join(""));

  const mapDescriptionToMetadata = (metadata: IBigQueryFieldMetadata, path: string[]) => {
    if (findDescription(path)) {
      metadata.description = findDescription(path).description;
    }

    if (metadata.fields) {
      metadata.fields = metadata.fields.map(nestedMetadata =>
        mapDescriptionToMetadata(nestedMetadata, [...path, nestedMetadata.name])
      );
    }

    return metadata;
  };

  const newMetadata = metadataArray.map(metaItem =>
    mapDescriptionToMetadata(metaItem, [metaItem.name])
  );
  return newMetadata;
}
