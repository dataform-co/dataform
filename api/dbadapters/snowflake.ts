import * as https from "https";
import * as PromisePool from "promise-pool-executor";

import { Credentials } from "df/api/commands/credentials";
import { collectEvaluationQueries, IDbAdapter } from "df/api/dbadapters/index";
import { parseSnowflakeEvalError } from "df/api/utils/error_parsing";
import { ErrorWithCause } from "df/common/errors/errors";
import { dataform } from "df/protos/ts";

interface ISnowflake {
  createConnection: (options: {
    account: string;
    username: string;
    password: string;
    database: string;
    warehouse: string;
    role: string;
  }) => ISnowflakeConnection;
}

interface ISnowflakeConnection {
  connect: (callback: (err: any, connection: ISnowflakeConnection) => void) => void;
  execute: (options: {
    sqlText: string;
    streamResult?: boolean;
    complete: (err: any, statement: ISnowflakeStatement, rows: any[]) => void;
  }) => void;
  destroy: (err: any) => void;
}

interface ISnowflakeStatement {
  cancel: () => void;
  streamRows: (options: { start?: number; end?: number }) => ISnowflakeResultStream;
}

interface ISnowflakeResultStream {
  on: (event: "error" | "data" | "end", handler: (data: Error | any[]) => void) => this;
}

export class SnowflakeDbAdapter implements IDbAdapter {
  public static async create(credentials: Credentials) {
    const connection = await connect(credentials as dataform.ISnowflake);
    return new SnowflakeDbAdapter(connection);
  }

  // Unclear exactly what snowflakes limit's are here, we can experiment with increasing this.
  private pool: PromisePool.PromisePoolExecutor = new PromisePool.PromisePoolExecutor({
    concurrencyLimit: 10,
    frequencyWindow: 1000,
    frequencyLimit: 10
  });

  constructor(private readonly connection: ISnowflakeConnection) {}

  public async execute(
    statement: string,
    options: {
      maxResults?: number;
    } = { maxResults: 1000 }
  ) {
    return {
      rows: await this.pool
        .addSingleTask({
          generator: () =>
            new Promise<any[]>((resolve, reject) => {
              this.connection.execute({
                sqlText: statement,
                streamResult: true,
                complete(err, stmt) {
                  if (err) {
                    reject(err);
                    return;
                  }
                  const rows: any[] = [];
                  const streamOptions =
                    !!options && !!options.maxResults
                      ? { start: 0, end: options.maxResults - 1 }
                      : {};
                  stmt
                    .streamRows(streamOptions)
                    .on("error", e => reject(e))
                    .on("data", row => rows.push(row))
                    .on("end", () => resolve(rows));
                }
              });
            })
        })
        .promise(),
      metadata: {}
    };
  }

  public async evaluate(
    queryOrAction: string | dataform.Table | dataform.Operation | dataform.Assertion,
    projectConfig?: dataform.ProjectConfig
  ) {
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
      { maxResults: 10000 }
    );
    return rows.map(row => ({
      database: row.TABLE_CATALOG,
      schema: row.TABLE_SCHEMA,
      name: row.TABLE_NAME
    }));
  }

  public async schemas(database: string): Promise<string[]> {
    const { rows } = await this.execute(
      `select SCHEMA_NAME from ${database ? `"${database}".` : ""}information_schema.schemata`
    );
    return rows.map(row => row.SCHEMA_NAME);
  }

  public table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    return Promise.all([
      this.execute(
        `
select column_name, data_type, is_nullable
from ${target.database ? `"${target.database}".` : ""}information_schema.columns
where table_schema = '${target.schema}' 
  and table_name = '${target.name}'`
      ),
      this.execute(
        `
select table_type
from ${target.database ? `"${target.database}".` : ""}information_schema.tables
where table_schema = '${target.schema}'
  and table_name = '${target.name}'`
      )
    ]).then(results => {
      if (results[1].rows.length > 0) {
        // The table exists.
        return {
          target,
          type: results[1].rows[0].TABLE_TYPE === "VIEW" ? "view" : "table",
          fields: results[0].rows.map(row => ({
            name: row.COLUMN_NAME,
            primitive: row.DATA_TYPE,
            flags: row.IS_NULLABLE && row.IS_NULLABLE === "YES" ? ["nullable"] : []
          }))
        };
      } else {
        return null;
      }
    });
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const { rows } = await this.execute(
      `SELECT * FROM "${target.schema}"."${target.name}" LIMIT ${limitRows}`
    );
    return rows;
  }

  public async prepareSchema(database: string, schema: string): Promise<void> {
    const schemas = await this.schemas(database);
    if (!schemas.includes(schema)) {
      await this.execute(
        `create schema if not exists ${database ? `"${database}".` : ""}"${schema}"`
      );
    }
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

  public async prepareStateMetadataTable(): Promise<void> {
    // Unimplemented.
  }

  public async persistedStateMetadata(): Promise<dataform.IPersistedTableMetadata[]> {
    const persistedMetadata: dataform.IPersistedTableMetadata[] = [];
    return persistedMetadata;
  }

  public async persistStateMetadata(actions: dataform.IExecutionAction[]) {
    // Unimplemented.
  }
  public async setMetadata(action: dataform.IExecutionAction): Promise<void> {
    // Unimplemented.
  }
  public async deleteStateMetadata(actions: dataform.IExecutionAction[]): Promise<void> {
    // Unimplemented.
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
      // This is horrible. However, it allows us to set the 'APPLICATION' parameter on client.environment,
      // which is passed all the way through to Snowflake's connection code. Pending a fix for
      // https://github.com/snowflakedb/snowflake-connector-nodejs/issues/100, this is the only way
      // we can achieve that.
      (require("snowflake-sdk/lib/core")({
        httpClientClass: require("snowflake-sdk/lib/http/node"),
        loggerClass: require("snowflake-sdk/lib/logger/node"),
        client: {
          version: require("snowflake-sdk/lib/util").driverVersion,
          environment: {
            ...process.versions,
            APPLICATION: "Dataform"
          }
        }
      }) as ISnowflake)
        .createConnection({
          account: snowflakeCredentials.accountId,
          username: snowflakeCredentials.username,
          password: snowflakeCredentials.password,
          database: snowflakeCredentials.databaseName,
          warehouse: snowflakeCredentials.warehouse,
          role: snowflakeCredentials.role
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
