import { Credentials } from "@dataform/api/commands/credentials";
import { IDbAdapter } from "@dataform/api/dbadapters/index";
import { dataform } from "@dataform/protos";

const Redshift: IRedshiftType = require("node-redshift");

interface IRedshiftConfig {
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  ssl: boolean;
}

interface IRedshiftType {
  query: (query: string) => Promise<{ rows: any[] }>;
  new (config: IRedshiftConfig): IRedshiftType;
}

export class RedshiftDbAdapter implements IDbAdapter {
  private client: IRedshiftType;

  constructor(credentials: Credentials) {
    const redshiftCredentials = credentials as dataform.IJDBC;
    const config: IRedshiftConfig = {
      host: redshiftCredentials.host,
      port: redshiftCredentials.port,
      user: redshiftCredentials.username,
      password: redshiftCredentials.password,
      database: redshiftCredentials.databaseName,
      ssl: true
    };
    this.client = new Redshift(config);
  }

  public execute(statement: string) {
    return this.client.query(statement).then(result => result.rows);
  }

  public evaluate(statement: string) {
    return this.client.query(`explain ${statement}`).then(() => {});
  }

  public tables(): Promise<dataform.ITarget[]> {
    return Promise.resolve()
      .then(() =>
        this.execute(
          `select table_name, table_schema
         from information_schema.tables
         where table_schema != 'information_schema'
           and table_schema != 'pg_catalog'
           and table_schema != 'pg_internal'`
        )
      )
      .then(rows =>
        rows.map(row => ({
          schema: row.table_schema,
          name: row.table_name
        }))
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
        `select table_type from information_schema.tables where table_schema = '${target.schema}' AND table_name = '${target.name}'`
      )
    ]).then(results => {
      if (results[1].length > 0) {
        // The table exists.
        return {
          target,
          type: results[1][0].table_type == "VIEW" ? "view" : "table",
          fields: results[0].map(row => ({
            name: row.column_name,
            primitive: row.data_type,
            flags: row.is_nullable && row.is_nullable == "YES" ? ["nullable"] : []
          }))
        };
      } else {
        throw new Error(`Could not find relation: ${target.schema}.${target.name}`);
      }
    });
  }

  public async preview(target: dataform.ITarget): Promise<any[]> {
    throw new Error("Method not yet implemented.");
  }

  public prepareSchema(schema: string): Promise<void> {
    return Promise.resolve().then(() =>
      this.execute(`create schema if not exists "${schema}"`).then(() => {})
    );
  }
}
