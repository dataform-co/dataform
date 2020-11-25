import Long from "long";
import { PromisePoolExecutor } from "promise-pool-executor";

import {
  BigQuery,
  GetTablesResponse,
  Table,
  TableField,
  TableMetadata
} from "@google-cloud/bigquery";
import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IDbClient, IExecutionResult, OnCancel } from "df/api/dbadapters/index";
import { parseBigqueryEvalError } from "df/api/utils/error_parsing";
import { LimitedResultSet } from "df/api/utils/results";
import { coerceAsError } from "df/common/errors/errors";
import { retry } from "df/common/promises";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

const EXTRA_GOOGLE_SCOPES = ["https://www.googleapis.com/auth/drive"];

const BIGQUERY_DATE_RELATED_FIELDS = [
  "BigQueryDate",
  "BigQueryTime",
  "BigQueryTimestamp",
  "BigQueryDatetime"
];

const BIGQUERY_INTERNAL_ERROR_JOB_MAX_ATTEMPTS = 3;

export class BigQueryDbAdapter implements IDbAdapter {
  public static async create(credentials: Credentials, options?: { concurrencyLimit?: number }) {
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
      params?: { [name: string]: any };
      onCancel?: OnCancel;
      interactive?: boolean;
      rowLimit?: number;
      byteLimit?: number;
      bigquery?: {
        labels?: { [label: string]: string };
      };
    } = { interactive: false, rowLimit: 1000, byteLimit: 1024 * 1024 }
  ): Promise<IExecutionResult> {
    if (options?.interactive && options?.bigquery?.labels) {
      throw new Error("BigQuery job labels may not be set for interactive queries.");
    }
    return this.pool
      .addSingleTask({
        generator: () =>
          options?.interactive
            ? this.runQuery(statement, options?.params, options?.rowLimit, options?.byteLimit)
            : this.createQueryJob(
                statement,
                options?.params,
                options?.rowLimit,
                options?.byteLimit,
                options?.onCancel,
                options?.bigquery?.labels
              )
      })
      .promise();
  }

  public async withClientLock<T>(callback: (client: IDbClient) => Promise<T>) {
    return await callback(this);
  }

  public async evaluate(queryOrAction: QueryOrAction, projectConfig?: dataform.ProjectConfig) {
    const validationQueries = collectEvaluationQueries(queryOrAction, true);

    return await Promise.all(
      validationQueries.map(async ({ query, incremental }) => {
        try {
          await this.pool
            .addSingleTask({
              generator: () =>
                this.getClient().query({
                  useLegacySql: false,
                  query,
                  dryRun: true
                })
            })
            .promise();
          return dataform.QueryEvaluation.create({
            status: dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS,
            incremental,
            query
          });
        } catch (e) {
          return {
            status: dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE,
            error: parseBigqueryEvalError(e),
            incremental,
            query
          };
        }
      })
    );
  }

  public async tables(): Promise<dataform.ITarget[]> {
    const datasets = await this.getClient().getDatasets({ autoPaginate: true, maxResults: 1000 });
    const tables = await Promise.all(
      datasets[0].map(dataset => dataset.getTables({ autoPaginate: true, maxResults: 1000 }))
    );
    const allTables: dataform.ITarget[] = [];
    tables.forEach((tablesResult: GetTablesResponse) =>
      tablesResult[0].forEach(table =>
        allTables.push({
          database: table.bigQuery.projectId,
          schema: table.dataset.id,
          name: table.id
        })
      )
    );
    return allTables;
  }

  public async search(
    searchText: string,
    options: { limit: number } = { limit: 1000 }
  ): Promise<dataform.ITableMetadata[]> {
    const results = await this.execute(
      `select table_catalog, table_schema, table_name
       from region-${this.bigQueryCredentials.location}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
       where regexp_contains(table_schema, @searchText) or regexp_contains(table_name, @searchText) or regexp_contains(field_path, @searchText)
       group by 1, 2, 3`,
      {
        params: {
          searchText: `(?i)${searchText}`
        },
        interactive: true,
        rowLimit: options.limit
      }
    );
    return await Promise.all(
      results.rows.map(row =>
        this.table({
          database: row.table_catalog,
          schema: row.table_schema,
          name: row.table_name
        })
      )
    );
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const metadata = await this.getMetadata(target);

    if (!metadata) {
      return null;
    }

    const metadataTarget = {
      database: metadata.tableReference.projectId,
      schema: metadata.tableReference.datasetId,
      name: metadata.tableReference.tableId
    };

    // Database isn't checked for equality as it's not always presented to the compiled graph, but
    // IS always available in the warehouse.
    if (
      metadata.tableReference.datasetId !== target.schema ||
      metadata.tableReference.tableId !== target.name
    ) {
      throw new Error(
        `Target ${JSON.stringify(metadataTarget)} does not match requested target ${JSON.stringify(
          target
        )}.`
      );
    }

    return dataform.TableMetadata.create({
      typeDeprecated: String(metadata.type).toLowerCase(),
      type:
        metadata.type === "TABLE"
          ? dataform.TableMetadata.Type.TABLE
          : metadata.type === "VIEW"
          ? dataform.TableMetadata.Type.VIEW
          : dataform.TableMetadata.Type.UNKNOWN,
      target: metadataTarget,
      fields: metadata.schema.fields?.map(field => convertField(field)),
      lastUpdatedMillis: Long.fromString(metadata.lastModifiedTime),
      description: metadata.description,
      labels: metadata.labels
    });
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const metadata = await this.getMetadata(target);
    if (metadata.type === "TABLE") {
      // For tables, we use the BigQuery tabledata.list API, as per https://cloud.google.com/bigquery/docs/best-practices-costs#preview-data.
      // Also see https://cloud.google.com/nodejs/docs/reference/bigquery/3.0.x/Table#getRows.
      const rowsResult = await this.getClient(target.database)
        .dataset(target.schema)
        .table(target.name)
        .getRows({
          maxResults: limitRows
        });
      return cleanRows(rowsResult[0]);
    }
    const {
      rows
    } = await this.execute(
      `SELECT * FROM \`${metadata.tableReference.projectId}.${metadata.tableReference.datasetId}.${metadata.tableReference.tableId}\``,
      { rowLimit: limitRows }
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

  public async setMetadata(action: dataform.IExecutionAction): Promise<void> {
    const { target, actionDescriptor } = action;

    const metadata = await this.getMetadata(target);
    const schemaWithDescription = addDescriptionToMetadata(
      actionDescriptor.columns,
      metadata.schema.fields
    );

    await this.getClient(target.database)
      .dataset(target.schema)
      .table(target.name)
      .setMetadata({
        description: actionDescriptor.description,
        schema: schemaWithDescription,
        labels: actionDescriptor.bigqueryLabels
      });
  }

  private async getMetadata(target: dataform.ITarget): Promise<TableMetadata> {
    try {
      const table = await this.getClient(target.database)
        .dataset(target.schema)
        .table(target.name)
        .getMetadata();
      return table?.[0];
    } catch (e) {
      if (e?.errors?.[0]?.reason === "notFound") {
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
          scopes: EXTRA_GOOGLE_SCOPES,
          location: this.bigQueryCredentials.location
        })
      );
    }
    return this.clients.get(projectId);
  }

  private async runQuery(
    query: string,
    params?: { [name: string]: any },
    rowLimit?: number,
    byteLimit?: number
  ) {
    const results = await new Promise<any[]>((resolve, reject) => {
      const allRows = new LimitedResultSet({
        rowLimit,
        byteLimit
      });
      const stream = this.getClient().createQueryStream({
        query,
        params
      });
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
    query: string,
    params?: { [name: string]: any },
    rowLimit?: number,
    byteLimit?: number,
    onCancel?: OnCancel,
    labels?: { [label: string]: string }
  ) {
    let isCancelled = false;
    onCancel?.(() => (isCancelled = true));

    return retry(
      async () => {
        try {
          const job = await this.getClient().createQueryJob({
            useLegacySql: false,
            jobPrefix: "dataform-",
            query,
            params,
            labels
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
      },
      BIGQUERY_INTERNAL_ERROR_JOB_MAX_ATTEMPTS,
      e => e.message?.includes("Retrying the job may solve the problem")
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

function convertField(field: TableField): dataform.IField {
  const result: dataform.IField = {
    name: field.name,
    flagsDeprecated: !!field.mode ? [field.mode] : [],
    flags: field.mode === "REPEATED" ? [dataform.Field.Flag.REPEATED] : [],
    description: field.description
  };
  if (field.type === "RECORD" || field.type === "STRUCT") {
    result.struct = dataform.Fields.create({
      fields: field.fields.map(innerField => convertField(innerField))
    });
  } else {
    result.primitive = convertFieldType(field.type);
  }
  return dataform.Field.create(result);
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
    case "GEOGRAPHY":
      return dataform.Field.Primitive.GEOGRAPHY;
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
