import { Credentials } from "@dataform/api/commands/credentials";
import { IDbAdapter } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";
import * as https from "https";

interface ISnowflakeStatement {
  cancel: () => void;
}

interface ISnowflakeConnection {
  connect: (callback: (err: any, connection: ISnowflakeConnection) => void) => void;
  execute: (options: {
    sqlText: string;
    complete: (err: any, statement: ISnowflakeStatement, rows: any[]) => void;
  }) => void;
}

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

const Snowflake: ISnowflake = require("snowflake-sdk");

export class SnowflakeDbAdapter implements IDbAdapter {
  private connection: ISnowflakeConnection;
  private connected: Promise<void>;

  constructor(credentials: Credentials) {
    const snowflakeCredentials = credentials as dataform.ISnowflake;
    this.connection = Snowflake.createConnection({
      account: snowflakeCredentials.accountId,
      username: snowflakeCredentials.username,
      password: snowflakeCredentials.password,
      database: snowflakeCredentials.databaseName,
      warehouse: snowflakeCredentials.warehouse,
      role: snowflakeCredentials.role
    });
    // We are forced to try our own HTTPS connection to the final <accountId>.snowflakecomputing.com URL
    // in order to verify its certificate. If we don't do this, and pass an invalid account ID (which thus
    // resolves to an invalid URL) to the snowflake connect() API, snowflake-sdk will not handle the
    // resulting error correctly (and thus crash this process).
    this.connected = this.verifyCertificate(snowflakeCredentials.accountId).then(
      () =>
        new Promise<void>((resolve, reject) => {
          this.connection.connect((err, conn) => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        })
    );
  }

  public async execute(statement: string) {
    await this.connected;
    return new Promise<any[]>((resolve, reject) => {
      this.connection.execute({
        sqlText: statement,
        complete(err, _, rows) {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        }
      });
    });
  }

  public evaluate(statement: string): Promise<void> {
    throw Error("Unimplemented");
  }

  public tables(): Promise<dataform.ITarget[]> {
    return Promise.resolve().then(() =>
      this.execute(
        `select table_name, table_schema
         from information_schema.tables
         where LOWER(table_schema) != 'information_schema'
           and LOWER(table_schema) != 'pg_catalog'
           and LOWER(table_schema) != 'pg_internal'`
      ).then(rows =>
        rows.map(row => ({
          schema: row.TABLE_SCHEMA,
          name: row.TABLE_NAME
        }))
      )
    );
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
        `select table_type from information_schema.tables where table_schema = '${
          target.schema
        }' AND table_name = '${target.name}'`
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

  private async verifyCertificate(accountId: string) {
    return new Promise<void>((resolve, reject) => {
      const req = https.request(`https://${accountId}.snowflakecomputing.com`);
      req.on("error", e => {
        reject(e);
      });
      req.end(() => {
        resolve();
      });
    });
  }
}
