import Long from "long";
import { PromisePoolExecutor } from "promise-pool-executor";

import { BigQuery, TableField, TableMetadata } from "@google-cloud/bigquery";
import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IExecutionResult, OnCancel } from "df/api/dbadapters/index";
import { parseBigqueryEvalError } from "df/api/utils/error_parsing";
import { LimitedResultSet } from "df/api/utils/results";
import {
  decodePersistedTableMetadata,
  encodePersistedTableMetadata,
  hashExecutionAction,
  IMetadataRow,
  toRowKey
} from "df/api/utils/run_cache";
import { coerceAsError } from "df/common/errors/errors";
import { StringifiedMap } from "df/common/strings/stringifier";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

const CACHED_STATE_TABLE_NAME = "dataform_meta.cache_state";

const EXTRA_GOOGLE_SCOPES = ["https://www.googleapis.com/auth/drive"];

const BIGQUERY_DATE_RELATED_FIELDS = [
  "BigQueryDate",
  "BigQueryTime",
  "BigQueryTimestamp",
  "BigQueryDatetime"
];

const MAX_QUERY_LENGTH = 1024 * 1024;

export class BigQueryDbAdapter implements IDbAdapter {
  public static async create(
    credentials: Credentials,
    _: string,
    options?: { concurrencyLimit?: number }
  ) {
    return new BigQueryDbAdapter(credentials, options);
  }

  private bigQueryCredentials: dataform.IBigQuery;
  private pool: PromisePoolExecutor;

  private readonly clients = new Map<string, BigQuery>();

  private constructor(credentials: Credentials, options?: { concurrencyLimit?: number }) {
    this.bigQueryCredentials = credentials as dataform.IBigQuery;
    // Bigquery allows 50 concurrent queries, and a rate limit of 100/user/second by default.
    // These limits should be safely low enough for most projects.
    this.pool = new PromisePoolExecutor({
      concurrencyLimit: options?.concurrencyLimit || 16,
      frequencyWindow: 1000,
      frequencyLimit: 30
    });
  }

  public async execute(
    statement: string,
    options: {
      onCancel?: OnCancel;
      interactive?: boolean;
      rowLimit?: number;
      byteLimit?: number;
    } = { interactive: false, rowLimit: 1000, byteLimit: 1024 * 1024 }
  ): Promise<IExecutionResult> {
    return this.pool
      .addSingleTask({
        generator: () =>
          options?.interactive
            ? this.runQuery(statement, options?.rowLimit, options?.byteLimit)
            : this.createQueryJob(
                statement,
                options?.rowLimit,
                options?.byteLimit,
                options?.onCancel
              )
      })
      .promise();
  }

  public async evaluate(queryOrAction: QueryOrAction, projectConfig?: dataform.ProjectConfig) {
    const validationQueries = collectEvaluationQueries(
      queryOrAction,
      projectConfig?.useSingleQueryPerAction === undefined ||
        !!projectConfig?.useSingleQueryPerAction
    );
    const queryEvaluations = new Array<dataform.IQueryEvaluation>();

    for (const { query, incremental } of validationQueries) {
      let evaluationResponse: dataform.IQueryEvaluation = {
        status: dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      };
      try {
        await this.getClient().query({
          useLegacySql: false,
          query,
          dryRun: true
        });
      } catch (e) {
        evaluationResponse = {
          status: dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE,
          error: parseBigqueryEvalError(e)
        };
      }
      queryEvaluations.push(
        dataform.QueryEvaluation.create({ ...evaluationResponse, incremental, query })
      );
    }
    return queryEvaluations;
  }

  public tables(): Promise<dataform.ITarget[]> {
    return this.getClient()
      .getDatasets({ autoPaginate: true, maxResults: 1000 })
      .then((result: any) => {
        return Promise.all(
          result[0].map((dataset: any) => {
            return this.pool
              .addSingleTask({
                generator: () => dataset.getTables({ autoPaginate: true, maxResults: 1000 })
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
      typeDeprecated: String(metadata.type).toLowerCase(),
      type:
        metadata.type === "TABLE"
          ? dataform.TableMetadata.Type.TABLE
          : metadata.type === "VIEW"
          ? dataform.TableMetadata.Type.VIEW
          : dataform.TableMetadata.Type.UNKNOWN,
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

  public async schemas(database: string): Promise<string[]> {
    const data = await this.getClient(database).getDatasets();
    return data[0].map(dataset => dataset.id);
  }

  public async createSchema(database: string, schema: string): Promise<void> {
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

  public async persistStateMetadata(
    transitiveInputMetadataByTarget: StringifiedMap<
      dataform.ITarget,
      dataform.PersistedTableMetadata.ITransitiveInputMetadata
    >,
    allActions: dataform.IExecutionAction[],
    actionsToPersist: dataform.IExecutionAction[],
    options: {
      onCancel: OnCancel;
    }
  ): Promise<void> {
    if (allActions.length === 0) {
      return;
    }
    try {
      // Create the cache table, if needed.
      await this.execute(
        `
CREATE TABLE IF NOT EXISTS \`${CACHED_STATE_TABLE_NAME}\` (
  target STRING,
  metadata_proto STRING
)`,
        options
      );
      // Before saving any new data, delete all entries for 'allActions'.
      await this.execute(
        `
DELETE \`${CACHED_STATE_TABLE_NAME}\` WHERE target IN (${allActions
          .map(({ target }) => `'${toRowKey(target)}'`)
          .join(",")})`,
        options
      );

      // Save entries for 'actionsToPersist'.
      const valuesTuples = actionsToPersist
        // If we were unable to load metadata for the action's output dataset, or for any of the action's
        // input datasets, do not store a cache entry for the action.
        .filter(
          action =>
            transitiveInputMetadataByTarget.has(action.target) &&
            action.transitiveInputs.every(transitiveInput =>
              transitiveInputMetadataByTarget.has(transitiveInput)
            )
        )
        .map(
          action =>
            `('${toRowKey(action.target)}', '${encodePersistedTableMetadata({
              target: action.target,
              lastUpdatedMillis: transitiveInputMetadataByTarget.get(action.target)
                .lastUpdatedMillis,
              definitionHash: hashExecutionAction(action),
              transitiveInputTables: action.transitiveInputs.map(transitiveInput =>
                transitiveInputMetadataByTarget.get(transitiveInput)
              )
            })}')`
        );
      // We have to split up the INSERT queries to get around BigQuery's query length limit.
      while (valuesTuples.length > 0) {
        let insertStatement = `INSERT INTO \`${CACHED_STATE_TABLE_NAME}\` (target, metadata_proto) VALUES ${valuesTuples.pop()}`;
        let nextInsertStatement = `${insertStatement}, ${valuesTuples[valuesTuples.length - 1]}`;
        while (valuesTuples.length > 0 && nextInsertStatement.length < MAX_QUERY_LENGTH) {
          insertStatement = nextInsertStatement;
          valuesTuples.pop();
          nextInsertStatement = `${insertStatement}, ${valuesTuples[valuesTuples.length - 1]}`;
        }
        await this.execute(insertStatement, options);
      }
    } catch (e) {
      if (String(e).includes("Exceeded rate limits")) {
        // Silently swallow rate-exceeded Errors; there's nothing we can do here, and they aren't harmful
        // (at worst, future runs may not cache as well as they could have).
        return;
      }
      throw e;
    }
  }

  public async setMetadata(action: dataform.IExecutionAction): Promise<any> {
    const { target, actionDescriptor, type, tableType } = action;

    if (!actionDescriptor || type !== "table" || tableType === "inline") {
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

  public async getMetadata(target: dataform.ITarget): Promise<TableMetadata> {
    return this.pool
      .addSingleTask({
        generator: async () => this.getMetadataOutsidePromisePool(target)
      })
      .promise();
  }

  private async getMetadataOutsidePromisePool(
    target: dataform.ITarget
  ): Promise<TableMetadata> {
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
      throw coerceAsError(e);
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

  private async runQuery(statement: string, rowLimit?: number, byteLimit?: number) {
    const results = await new Promise<any[]>((resolve, reject) => {
      const allRows = new LimitedResultSet({
        rowLimit,
        byteLimit
      });
      const stream = this.getClient().createQueryStream(statement);
      stream
        .on("error", e => reject(coerceAsError(e)))
        .on("data", row => {
          if (!allRows.push(row)) {
            stream.end();
          }
        })
        .on("end", () => {
          resolve(allRows.rows);
        });
    });
    return { rows: cleanRows(results), metadata: {} };
  }

  private async createQueryJob(
    statement: string,
    rowLimit?: number,
    byteLimit?: number,
    onCancel?: OnCancel
  ) {
    let isCancelled = false;
    onCancel?.(() => (isCancelled = true));

    try {
      const job = await this.getClient().createQueryJob({
        useLegacySql: false,
        jobPrefix: "dataform-",
        query: statement
      });
      const resultStream = job[0].getQueryResultsStream();
      return new Promise<IExecutionResult>((resolve, reject) => {
        if (isCancelled) {
          resultStream.end();
          reject(new Error("Query cancelled."));
          return;
        }
        onCancel?.(() => {
          resultStream.end();
          reject(new Error("Query cancelled."));
        });

        const results = new LimitedResultSet({
          rowLimit,
          byteLimit
        });
        resultStream
          .on("error", e => reject(e))
          .on("data", row => {
            if (!results.push(row)) {
              resultStream.end();
            }
          })
          .on("end", async () => {
            try {
              const [jobMetadata] = await job[0].getMetadata();
              if (!!jobMetadata.status?.errorResult) {
                reject(new Error(jobMetadata.status.errorResult.message));
                return;
              }
              resolve({
                rows: results.rows,
                metadata: {
                  bigquery: {
                    jobId: jobMetadata.jobReference.jobId,
                    totalBytesBilled: jobMetadata.statistics.query.totalBytesBilled
                      ? Long.fromString(jobMetadata.statistics.query.totalBytesBilled)
                      : Long.ZERO,
                    totalBytesProcessed: jobMetadata.statistics.query.totalBytesProcessed
                      ? Long.fromString(jobMetadata.statistics.query.totalBytesProcessed)
                      : Long.ZERO
                  }
                }
              });
            } catch (e) {
              reject(e);
            }
          });
      });
    } catch (e) {
      throw coerceAsError(e);
    }
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

function convertField(field: TableField): dataform.IField {
  const result: dataform.IField = {
    name: field.name,
    flagsDeprecated: !!field.mode ? [field.mode] : [],
    flags: field.mode === "REPEATED" ? [dataform.Field.Flag.REPEATED] : [],
    description: field.description
  };
  if (field.type === "RECORD" || field.type === "STRUCT") {
    result.struct = { fields: field.fields.map(innerField => convertField(innerField)) };
  } else {
    result.primitiveDeprecated = field.type;
    result.primitive = convertFieldType(field.type);
  }
  return result;
}

// See: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema
function convertFieldType(type: string) {
  switch (String(type).toUpperCase()) {
    case "FLOAT":
    case "FLOAT64":
      return dataform.Field.Primitive.FLOAT;
    case "INTEGER":
    case "INT64":
      return dataform.Field.Primitive.INTEGER;
    case "NUMERIC":
      return dataform.Field.Primitive.NUMERIC;
    case "BOOL":
    case "BOOLEAN":
      return dataform.Field.Primitive.BOOLEAN;
    case "STRING":
      return dataform.Field.Primitive.STRING;
    case "DATE":
      return dataform.Field.Primitive.DATE;
    case "DATETIME":
      return dataform.Field.Primitive.DATETIME;
    case "TIMESTAMP":
      return dataform.Field.Primitive.TIMESTAMP;
    case "TIME":
      return dataform.Field.Primitive.TIME;
    case "BYTES":
      return dataform.Field.Primitive.BYTES;
    default:
      return dataform.Field.Primitive.UNKNOWN;
  }
}

function addDescriptionToMetadata(
  columnDescriptions: dataform.IColumnDescriptor[],
  metadataArray: TableField[]
): TableField[] {
  const findDescription = (path: string[]) =>
    columnDescriptions.find(column => column.path.join("") === path.join(""));

  const mapDescriptionToMetadata = (metadata: TableField, path: string[]) => {
    const description = findDescription(path);
    if (description) {
      metadata.description = description.description;
      if (description.bigqueryPolicyTags?.length > 0) {
        metadata.policyTags = {
          names: description.bigqueryPolicyTags
        };
      }
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
