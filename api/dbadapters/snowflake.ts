import * as https from "https";
import * as PromisePool from "promise-pool-executor";
import { Readable } from "stream";

import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IDbClient, OnCancel } from "df/api/dbadapters/index";
import { parseSnowflakeEvalError } from "df/api/utils/error_parsing";
import { LimitedResultSet } from "df/api/utils/results";
import { ErrorWithCause } from "df/common/errors/errors";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

const HEARTBEAT_INTERVAL_SECONDS = 30;

// This is horrible. However, it allows us to set the 'APPLICATION' parameter on client.environment,
// which is passed all the way through to Snowflake's connection code. Pending a fix for
// https://github.com/snowflakedb/snowflake-connector-nodejs/issues/100, this is the only way
// we can achieve that.
// tslint:disable-next-line: no-var-requires
const snowflake = require("snowflake-sdk/lib/core")({
  httpClientClass: require("snowflake-sdk/lib/http/node"),
  loggerClass: require("snowflake-sdk/lib/logger/node"),
  client: {
    version: require("snowflake-sdk/lib/util").driverVersion,
    environment: {
      ...process.versions,
      APPLICATION: "Dataform"
    }
  }
}) as ISnowflake;
snowflake.configure({ logLevel: "trace" });

interface ISnowflake {
  configure: (options: { logLevel: string }) => void;
  createConnection: (options: {
    account: string;
    username: string;
    password: string;
    database: string;
    warehouse: string;
    role: string;
    clientSessionKeepAlive: boolean;
    clientSessionKeepAliveHeartbeatFrequency: number;
  }) => ISnowflakeConnection;
}

interface ISnowflakeConnection {
  connect: (callback: (err: any, connection: ISnowflakeConnection) => void) => void;
  execute: (options: {
    sqlText: string;
    binds?: any[];
    streamResult?: boolean;
    complete: (err: any, statement: ISnowflakeStatement, rows: any[]) => void;
  }) => void;
  destroy: (err: any) => void;
}

interface ISnowflakeStatement {
  cancel: (err: any) => void;
  streamRows: (options?: { start?: number; end?: number }) => Readable;
}

export class SnowflakeDbAdapter implements IDbAdapter {
  public static async create(credentials: Credentials, options?: { concurrencyLimit?: number }) {
    const connection = await connect(credentials as dataform.ISnowflake);
    return new SnowflakeDbAdapter(connection, options);
  }

  // Unclear exactly what snowflakes limit's are here, we can experiment with increasing this.
  private pool: PromisePool.PromisePoolExecutor;

  constructor(
    private readonly connection: ISnowflakeConnection,
    options?: { concurrencyLimit?: number }
  ) {
    this.pool = new PromisePool.PromisePoolExecutor({
      concurrencyLimit: options?.concurrencyLimit || 10,
      frequencyWindow: 1000,
      frequencyLimit: 10
    });
  }

  public async execute(
    statement: string,
    options: {
      binds?: any[];
      onCancel?: OnCancel;
      rowLimit?: number;
      byteLimit?: number;
    } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
  ) {
    return {
      rows: await this.pool
        .addSingleTask({
          generator: () =>
            new Promise<any[]>((resolve, reject) => {
              this.connection.execute({
                sqlText: statement,
                binds: options?.binds,
                streamResult: true,
                complete(err, stmt) {
                  if (err) {
                    let message = `Snowflake SQL query failed: ${err.message}.`;
                    if (err.cause) {
                      message += ` Root cause: ${err.cause}`;
                    }
                    reject(new ErrorWithCause(message, err));
                    return;
                  }
                  options?.onCancel?.(() =>
                    stmt.cancel((e: any) => {
                      if (e) {
                        reject(e);
                      }
                    })
                  );
                  const results = new LimitedResultSet({
                    rowLimit: options?.rowLimit,
                    byteLimit: options?.byteLimit
                  });
                  const stream = stmt.streamRows();
                  stream
                    .on("error", e => reject(e))
                    .on("data", row => {
                      if (!results.push(row)) {
                        stream.destroy();
                      }
                    })
                    .on("end", () => resolve(results.rows))
                    .on("close", () => resolve(results.rows));
                }
              });
            })
        })
        .promise(),
      metadata: {}
    };
  }

  public async withClientLock<T>(callback: (client: IDbClient) => Promise<T>) {
    return await callback(this);
  }

  public async evaluate(queryOrAction: QueryOrAction, projectConfig?: dataform.ProjectConfig) {
    const validationQueries = collectEvaluationQueries(queryOrAction, false, (query: string) =>
      !!query ? `select system$explain_plan_json($$${query}$$)` : ""
    ).map((validationQuery, index) => ({ index, validationQuery }));
    const validationQueriesWithoutWrappers = collectEvaluationQueries(queryOrAction, false);

    const queryEvaluations = new Array<dataform.IQueryEvaluation>();
    for (const { index, validationQuery } of validationQueries) {
      let evaluationResponse: dataform.IQueryEvaluation = {
        status: dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      };
      try {
        await this.execute(validationQuery.query);
      } catch (e) {
        evaluationResponse = {
          status: dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE,
          error: parseSnowflakeEvalError(e.message)
        };
      }
      queryEvaluations.push(
        dataform.QueryEvaluation.create({
          ...evaluationResponse,
          incremental: validationQuery.incremental,
          query: validationQueriesWithoutWrappers[index].query
        })
      );
    }
    return queryEvaluations;
  }

  public async tables(): Promise<dataform.ITarget[]> {
    const { rows } = await this.execute(
      `
select table_name, table_schema, table_catalog
from information_schema.tables
where LOWER(table_schema) != 'information_schema'
  and LOWER(table_schema) != 'pg_catalog'
  and LOWER(table_schema) != 'pg_internal'`,
      { rowLimit: 10000 }
    );
    return rows.map(row => ({
      database: row.TABLE_CATALOG,
      schema: row.TABLE_SCHEMA,
      name: row.TABLE_NAME
    }));
  }

  public async search(searchText: string): Promise<dataform.ITableMetadata[]> {
    const databases = await this.execute(`select database_name from information_schema.databases`, {
      rowLimit: 100
    });
    const allTables = databases.rows
      .map(row => `select * from ${row.DATABASE_NAME}.information_schema.tables`)
      .join(" union all ");
    const allColumns = databases.rows
      .map(row => `select * from ${row.DATABASE_NAME}.information_schema.columns`)
      .join(" union all ");
    const results = await this.execute(
      `select tables.table_catalog as table_catalog, tables.table_schema as table_schema, tables.table_name as table_name
       from (${allTables}) as tables
       left join (${allColumns}) as columns on tables.table_catalog = columns.table_catalog and tables.table_schema = columns.table_schema
         and tables.table_name = columns.table_name
       where LOWER(tables.table_catalog) like :1 or LOWER(tables.table_schema) like :1 or LOWER(tables.table_name) like :1 or tables.comment like :1
         or LOWER(columns.column_name) like :1 or columns.comment like :1
       group by 1, 2, 3
       `,
      {
        binds: [`%${searchText}%`],
        rowLimit: 100
      }
    );
    return await Promise.all(
      results.rows.map(row =>
        this.table({
          database: row.TABLE_CATALOG,
          schema: row.TABLE_SCHEMA,
          name: row.TABLE_NAME
        })
      )
    );
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const [tableResults, columnResults] = await Promise.all([
      this.execute(
        `
select table_type, comment
from ${target.database ? `"${target.database}".` : ""}information_schema.tables
where table_schema = '${target.schema}'
  and table_name = '${target.name}'`
      ),
      this.execute(
        `
select column_name, data_type, is_nullable, comment
from ${target.database ? `"${target.database}".` : ""}information_schema.columns
where table_schema = '${target.schema}' 
  and table_name = '${target.name}'`
      )
    ]);
    if (tableResults.rows.length === 0) {
      // The table does not exist.
      return null;
    }

    return dataform.TableMetadata.create({
      target,
      typeDeprecated: tableResults.rows[0].TABLE_TYPE === "VIEW" ? "view" : "table",
      type:
        tableResults.rows[0].TABLE_TYPE === "VIEW"
          ? dataform.TableMetadata.Type.VIEW
          : dataform.TableMetadata.Type.TABLE,
      fields: columnResults.rows.map(row =>
        dataform.Field.create({
          name: row.COLUMN_NAME,
          primitiveDeprecated: row.DATA_TYPE,
          primitive: convertFieldType(row.DATA_TYPE),
          flagsDeprecated: row.IS_NULLABLE && row.IS_NULLABLE === "YES" ? ["nullable"] : [],
          flags: row.DATA_TYPE === "ARRAY" ? [dataform.Field.Flag.REPEATED] : [],
          description: row.COMMENT
        })
      ),
      description: tableResults.rows[0].COMMENT
    });
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const { rows } = await this.execute(
      `SELECT * FROM "${target.schema}"."${target.name}" LIMIT ${limitRows}`
    );
    return rows;
  }

  public async schemas(database: string): Promise<string[]> {
    const { rows } = await this.execute(
      `select SCHEMA_NAME from ${database ? `"${database}".` : ""}information_schema.schemata`
    );
    return rows.map(row => row.SCHEMA_NAME);
  }

  public async createSchema(database: string, schema: string): Promise<void> {
    await this.execute(
      `create schema if not exists ${database ? `"${database}".` : ""}"${schema}"`
    );
  }

  public async close() {
    await new Promise((resolve, reject) => {
      this.connection.destroy((err: any) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  public async persistedStateMetadata(): Promise<dataform.IPersistedTableMetadata[]> {
    return [];
  }

  public async persistStateMetadata() {
    // Unimplemented.
  }

  public async setMetadata(action: dataform.IExecutionAction): Promise<void> {
    const { target, actionDescriptor, tableType } = action;

    const queries: Array<Promise<any>> = [];
    if (actionDescriptor.description) {
      queries.push(
        this.execute(
          `comment on ${tableType === "view" ? "view" : "table"} ${
            target.database ? `"${target.database}".` : ""
          }"${target.schema}"."${target.name}" is '${actionDescriptor.description.replace(
            "'",
            "\\'"
          )}'`
        )
      );
    }
    if (tableType !== "view" && actionDescriptor.columns?.length > 0) {
      actionDescriptor.columns
        .filter(column => column.path.length === 1)
        .forEach(column => {
          queries.push(
            this.execute(
              `comment if exists on column ${target.database ? `"${target.database}".` : ""}"${
                target.schema
              }"."${target.name}"."${column.path[0]}" is '${column.description.replace(
                "'",
                "\\'"
              )}'`
            )
          );
        });
    }

    await Promise.all(queries);
  }
}

async function connect(snowflakeCredentials: dataform.ISnowflake) {
  // We are forced to try our own HTTPS connection to the final <accountId>.snowflakecomputing.com URL
  // in order to verify its certificate. If we don't do this, and pass an invalid account ID (which thus
  // resolves to an invalid URL) to the snowflake connect() API, snowflake-sdk will not handle the
  // resulting error correctly (and thus crash this process).
  await testHttpsConnection(`https://${snowflakeCredentials.accountId}.snowflakecomputing.com`);
  try {
    return await new Promise<ISnowflakeConnection>((resolve, reject) => {
      snowflake
        .createConnection({
          account: snowflakeCredentials.accountId,
          username: snowflakeCredentials.username,
          password: snowflakeCredentials.password,
          database: snowflakeCredentials.databaseName,
          warehouse: snowflakeCredentials.warehouse,
          role: snowflakeCredentials.role,
          clientSessionKeepAlive: true,
          clientSessionKeepAliveHeartbeatFrequency: HEARTBEAT_INTERVAL_SECONDS
        })
        .connect((err, conn) => {
          if (err) {
            reject(err);
          } else {
            resolve(conn);
          }
        });
    });
  } catch (e) {
    throw new ErrorWithCause(`Could not connect to Snowflake: ${e.message}`, e);
  }
}

async function testHttpsConnection(url: string) {
  try {
    await new Promise<void>((resolve, reject) => {
      const req = https.request(url);
      req.on("error", e => {
        reject(e);
      });
      req.end(() => {
        resolve();
      });
    });
  } catch (e) {
    throw new ErrorWithCause(`Could not open HTTPS connection to ${url}.`, e);
  }
}

// See https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
function convertFieldType(type: string) {
  switch (String(type).toUpperCase()) {
    case "FLOAT":
    case "FLOAT4":
    case "FLOAT8":
    case "DOUBLE":
    case "DOUBLE PRECISION":
    case "REAL":
      return dataform.Field.Primitive.FLOAT;
    case "INTEGER":
    case "INT":
    case "BIGINT":
    case "SMALLINT":
      return dataform.Field.Primitive.INTEGER;
    case "NUMBER":
    case "DECIMAL":
    case "NUMERIC":
      return dataform.Field.Primitive.NUMERIC;
    case "BOOLEAN":
      return dataform.Field.Primitive.BOOLEAN;
    case "STRING":
    case "VARCHAR":
    case "CHAR":
    case "CHARACTER":
    case "TEXT":
      return dataform.Field.Primitive.STRING;
    case "DATE":
      return dataform.Field.Primitive.DATE;
    case "DATETIME":
      return dataform.Field.Primitive.DATETIME;
    case "TIMESTAMP":
    case "TIMESTAMP_LTZ":
    case "TIMESTAMP_NTZ":
    case "TIMESTAMP_TZ":
      return dataform.Field.Primitive.TIMESTAMP;
    case "TIME":
      return dataform.Field.Primitive.TIME;
    case "BINARY":
    case "VARBINARY":
      return dataform.Field.Primitive.BYTES;
    case "VARIANT":
    case "ARRAY":
    case "OBJECT":
      return dataform.Field.Primitive.ANY;
    default:
      return dataform.Field.Primitive.UNKNOWN;
  }
}
