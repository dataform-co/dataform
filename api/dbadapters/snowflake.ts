import { Credentials } from "@dataform/api/commands/credentials";
import { DbAdapter } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";

interface ISnowflakeStatement {
  cancel: () => void;
}

interface ISnowflakeConnection {
  connect: (callback: (err: any, connection: ISnowflakeConnection) => void) => void;
  execute: (
    options: {
      sqlText: string;
      complete: (err: any, statement: ISnowflakeStatement, rows: any[]) => void;
    }
  ) => void;
}

interface ISnowflake {
  createConnection: (
    options: {
      account: string;
      username: string;
      password: string;
      database: string;
      warehouse: string;
      role: string;
    }
  ) => ISnowflakeConnection;
}

const Snowflake: ISnowflake = require("snowflake-sdk");

export class SnowflakeDbAdapter implements DbAdapter {
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
    this.connected = new Promise((resolve, reject) => {
      this.connection.connect((err, conn) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
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

  public async tables(): Promise<dataform.ITarget[]> {
    const rows = await this.execute(
      `select table_name, table_schema
       from information_schema.tables
       where LOWER(table_schema) != 'information_schema'
         and LOWER(table_schema) != 'pg_catalog'
         and LOWER(table_schema) != 'pg_internal'`
    );
    return rows.map(row => ({
      schema: row.TABLE_SCHEMA,
      name: row.TABLE_NAME
    }));
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const tableMetadataFuture = this.execute(
      `select table_type from information_schema.tables where table_schema = '${
        target.schema
      }' AND table_name = '${target.name}'`
    );
    const columnMetadataFuture = this.execute(
      `select column_name, data_type, is_nullable
     from information_schema.columns
     where table_schema = '${target.schema}' AND table_name = '${target.name}'`
    );
    const tableMetadata = await tableMetadataFuture;
    const columnMetadata = await columnMetadataFuture;
    if (tableMetadata.length === 0) {
      throw new Error(`Could not find relation: ${target.schema}.${target.name}`);
    }
    // The table exists.
    return {
      target,
      type: tableMetadata[0].TABLE_TYPE === "VIEW" ? "view" : "table",
      fields: columnMetadata.map(row => ({
        name: row.COLUMN_NAME,
        primitive: row.DATA_TYPE,
        flags: row.IS_NULLABLE && row.IS_NULLABLE === "YES" ? ["nullable"] : []
      }))
    };
  }

  public async prepareSchema(schema: string): Promise<void> {
    await this.execute(`create schema if not exists "${schema}"`);
  }
}
