import { Credentials } from "@dataform/api/commands/credentials";
import { IDbAdapter } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";
import * as https from "https";
import * as PromisePool from "promise-pool-executor";

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
}

interface ISnowflakeStatement {
  cancel: () => void;
  streamRows: (options: { start?: number; end?: number }) => ISnowflakeResultStream;
}

interface ISnowflakeResultStream {
  on: (event: "error" | "data" | "end", handler: (data: Error | any[]) => void) => this;
}

const snowflake: ISnowflake = require("snowflake-sdk");

export class SnowflakeDbAdapter implements IDbAdapter {
  private connectionPromise: Promise<ISnowflakeConnection>;
  private pool: PromisePool.PromisePoolExecutor;

  constructor(credentials: Credentials) {
    this.connectionPromise = connect(credentials as dataform.ISnowflake);
    // Unclear exactly what snowflakes limit's are here, we can experiment with increasing this.
    this.pool = new PromisePool.PromisePoolExecutor({
      concurrencyLimit: 10,
      frequencyWindow: 1000,
      frequencyLimit: 10
    });
  }

  public async execute(
    statement: string,
    options: {
      maxResults?: number;
    } = { maxResults: 1000 }
  ) {
    const connection = await this.connectionPromise;
    return this.pool
      .addSingleTask({
        generator: () =>
          new Promise<any[]>((resolve, reject) => {
            connection.execute({
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
      .promise();
  }

  public evaluate(statement: string): Promise<void> {
    throw new Error("Unimplemented");
  }

  public async tables(): Promise<dataform.ITarget[]> {
    const rows = await this.execute(
      `select table_name, table_schema
       from information_schema.tables
       where LOWER(table_schema) != 'information_schema'
         and LOWER(table_schema) != 'pg_catalog'
         and LOWER(table_schema) != 'pg_internal'`,
      { maxResults: 10000 }
    );
    return rows.map(row => ({
      schema: row.TABLE_SCHEMA,
      name: row.TABLE_NAME
    }));
  }

  public async schemas(): Promise<string[]> {
    const rows = await this.execute(`select SCHEMA_NAME from information_schema.schemata`);
    return rows.map(row => row.SCHEMA_NAME);
  }

  public table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    return Promise.all([
      this.execute(
        `select column_name, data_type, is_nullable
       from information_schema.columns
       where table_schema = '${target.schema}' AND table_name = '${target.name}'`
      ),
      this.execute(
        `select table_type from information_schema.tables where table_schema = '${target.schema}' AND table_name = '${target.name}'`
      )
    ]).then(results => {
      if (results[1].length > 0) {
        // The table exists.
        return {
          target,
          type: results[1][0].TABLE_TYPE == "VIEW" ? "view" : "table",
          fields: results[0].map(row => ({
            name: row.COLUMN_NAME,
            primitive: row.DATA_TYPE,
            flags: row.IS_NULLABLE && row.IS_NULLABLE == "YES" ? ["nullable"] : []
          }))
        };
      } else {
        throw new Error(`Could not find relation: ${target.schema}.${target.name}`);
      }
    });
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    return this.execute(`SELECT * FROM "${target.schema}"."${target.name}" LIMIT ${limitRows}`);
  }

  public async prepareSchema(schema: string): Promise<void> {
    const schemas = await this.schemas();
    if (!schemas.includes(schema)) {
      await this.execute(`create schema if not exists "${schema}"`);
    }
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
    throw new Error(`Could not connect to Snowflake: ${e.message}`);
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
    throw new Error(`Could not open HTTPS connection to ${url}: ${e.message}`);
  }
}
