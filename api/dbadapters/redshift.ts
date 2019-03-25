import * as protos from "@dataform/protos";
import { DbAdapter } from "./index";

const Redshift: RedshiftType = require("node-redshift");

interface RedshiftType {
  query: (query: string) => Promise<{ rows: any[] }>;
  new (client: {
    host?: string;
    port?: number | Long;
    user?: string;
    password?: string;
    database?: string;
    ssl: boolean;
  }): RedshiftType;
}

export class RedshiftDbAdapter implements DbAdapter {
  private client: RedshiftType;

  constructor(profile: protos.IProfile) {
    this.client = new Redshift(Object.assign({ ssl: true }, profile.redshift));
  }

  public execute(statement: string) {
    return this.client.query(statement).then(result => result.rows);
  }

  public evaluate(statement: string) {
    return this.client.query(`explain ${statement}`).then(() => {});
  }

  public tables(): Promise<protos.ITarget[]> {
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

  public table(target: protos.ITarget): Promise<protos.ITableMetadata> {
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

  public prepareSchema(schema: string): Promise<void> {
    return Promise.resolve().then(() =>
      this.execute(`create schema if not exists "${schema}"`).then(() => {})
    );
  }
}
