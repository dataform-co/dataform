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
    this.connection.connect((err, conn) => {
      if (err) {
        console.error("Unable to connect: " + err.message);
      }
    });
  }

  public execute(statement: string) {
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

  public prepareSchema(schema: string): Promise<void> {
    return Promise.resolve().then(() =>
      this.execute(`create schema if not exists "${schema}"`).then(() => {})
    );
  }
}
